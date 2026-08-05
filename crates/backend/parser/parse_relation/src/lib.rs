#![allow(non_snake_case)]

#[cfg(test)]
mod tests;

use mcx::{Mcx, PgVec};
use nodes_core::node_funcs;
use parser_small1::{
    parser_errposition, ParseExprKind, ParseNamespaceColumn, ParseNamespaceItem, ParseState,
};
use types_core::{AttrNumber, Index, InvalidOid, Oid, OidIsValid, ParseLoc};
use types_core::catalog::{RECORDARRAYOID, RECORDOID};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_AMBIGUOUS_ALIAS, ERRCODE_AMBIGUOUS_COLUMN,
    ERRCODE_DUPLICATE_ALIAS, ERRCODE_INVALID_COLUMN_REFERENCE, ERRCODE_UNDEFINED_COLUMN,
    ERRCODE_UNDEFINED_TABLE, ERROR,
};
use types_nodes::parsenodes::ACL_SELECT;
use types_nodes::{
    Alias, Node, NodeList, NodeTag, RTEKind, RTEPermissionInfo, RangeTblEntry, Var,
    VarReturningType,
};
use types_rel::{AccessShareLock, NoLock, Relation, RowShareLock, LOCKMODE};
use types_tuple::htup::{FirstLowInvalidHeapAttributeNumber, TableOidAttributeNumber};
use types_tuple::tupdesc::TupleDescData;

#[allow(non_upper_case_globals)]
const InvalidAttrNumber: AttrNumber = 0;

const MAX_FUZZY_DISTANCE: i32 = 3;

pub fn init_seams() {
    parse_relation_seams::expand_nsitem_vars_at::set(expand_nsitem_vars_at);
}

fn expand_nsitem_vars_at<'a, 'p, 'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &'a ParseState<'p, 'mcx>,
    varno: i32,
    sublevels_up: i32,
    location: ParseLoc,
) -> PgResult<NodeList<'mcx>> {
    let nsitem = GetNSItemByRangeTablePosn(pstate, varno, sublevels_up);
    Ok(expandNSItemVars(mcx, pstate, nsitem, sublevels_up, location)?.0)
}

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

fn errpos(pstate: &ParseState<'_, '_>, location: ParseLoc) -> i32 {
    parser_errposition(pstate, location, mbutils::GetDatabaseEncoding())
}

pub fn refnameNamespaceItem<'p, 'mcx>(
    pstate: &'p ParseState<'p, 'mcx>,
    schemaname: Option<&str>,
    refname: &str,
    location: ParseLoc,
    mut sublevels_up: Option<&mut i32>,
) -> PgResult<Option<&'mcx ParseNamespaceItem<'mcx>>> {
    let mut relId = InvalidOid;

    if let Some(su) = sublevels_up.as_deref_mut() {
        *su = 0;
    }

    if let Some(schemaname) = schemaname {
        let namespaceId = catalog_namespace::LookupNamespaceNoError(schemaname)?;
        if !OidIsValid(namespaceId) {
            return Ok(None);
        }
        relId = lsyscache::get_relname_relid(refname, namespaceId)?;
        if !OidIsValid(relId) {
            return Ok(None);
        }
    }

    let mut ps = Some(pstate);
    while let Some(p) = ps {
        let result = if OidIsValid(relId) {
            scanNameSpaceForRelid(p, relId, location)?
        } else {
            scanNameSpaceForRefname(p, refname, location)?
        };
        if result.is_some() {
            return Ok(result);
        }
        match sublevels_up.as_deref_mut() {
            Some(su) => *su += 1,
            None => break,
        }
        ps = p.parentParseState;
    }
    Ok(None)
}

fn scanNameSpaceForRefname<'p, 'mcx>(
    pstate: &'p ParseState<'p, 'mcx>,
    refname: &str,
    location: ParseLoc,
) -> PgResult<Option<&'mcx ParseNamespaceItem<'mcx>>> {
    let mut result: Option<&'mcx ParseNamespaceItem<'mcx>> = None;
    for nsitem in pstate.p_namespace.iter().copied() {
        if !nsitem.p_rel_visible {
            continue;
        }
        if nsitem.p_lateral_only.get() && !pstate.p_lateral_active {
            continue;
        }
        if nsitem.p_names.aliasname == Some(refname) {
            if result.is_some() {
                return Err(ambiguous_table_ref(pstate, refname, location));
            }
            check_lateral_ref_ok(pstate, nsitem, location)?;
            result = Some(nsitem);
        }
    }
    Ok(result)
}

fn scanNameSpaceForRelid<'p, 'mcx>(
    pstate: &'p ParseState<'p, 'mcx>,
    relid: Oid,
    location: ParseLoc,
) -> PgResult<Option<&'mcx ParseNamespaceItem<'mcx>>> {
    let mut result: Option<&'mcx ParseNamespaceItem<'mcx>> = None;
    for nsitem in pstate.p_namespace.iter().copied() {
        let rte = nsitem.rte();
        if !nsitem.p_rel_visible {
            continue;
        }
        if nsitem.p_lateral_only.get() && !pstate.p_lateral_active {
            continue;
        }
        if nsitem.p_returning_type != VarReturningType::VAR_RETURNING_DEFAULT {
            continue;
        }
        if rte.rtekind == RTEKind::RTE_RELATION && rte.relid == relid && rte.alias.is_none() {
            if result.is_some() {
                return Err(ambiguous_table_relid(pstate, relid, location));
            }
            check_lateral_ref_ok(pstate, nsitem, location)?;
            result = Some(nsitem);
        }
    }
    Ok(result)
}

fn check_lateral_ref_ok<'p, 'mcx>(
    pstate: &'p ParseState<'p, 'mcx>,
    nsitem: &ParseNamespaceItem<'mcx>,
    location: ParseLoc,
) -> PgResult<()> {
    if nsitem.p_lateral_only.get() && !nsitem.p_lateral_ok.get() {
        return Err(bad_lateral_ref(pstate, nsitem, location));
    }
    Ok(())
}

pub fn GetNSItemByRangeTablePosn<'p, 'mcx>(
    pstate: &'p ParseState<'p, 'mcx>,
    varno: i32,
    sublevels_up: i32,
) -> &'mcx ParseNamespaceItem<'mcx> {
    let mut p = pstate;
    for _ in 0..sublevels_up {
        p = p.parentParseState.expect("sublevels_up exceeds pstate depth");
    }
    for nsitem in p.p_namespace.iter().copied() {
        if nsitem.p_rtindex == varno {
            return nsitem;
        }
    }
    panic!("nsitem not found (internal error)");
}

pub fn GetRTEByRangeTablePosn<'p, 'mcx>(
    pstate: &'p ParseState<'p, 'mcx>,
    varno: i32,
    sublevels_up: i32,
) -> &'mcx RangeTblEntry<'mcx> {
    let mut p = pstate;
    for _ in 0..sublevels_up {
        p = p.parentParseState.expect("sublevels_up exceeds pstate depth");
    }
    debug_assert!(varno > 0 && varno as usize <= p.p_rtable.len());
    p.p_rtable.nth(varno as usize - 1).as_range_tbl_entry().expect("rtable holds RangeTblEntry")
}

pub fn scanNSItemForColumn<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    nsitem: &ParseNamespaceItem<'mcx>,
    sublevels_up: i32,
    colname: &str,
    location: ParseLoc,
) -> PgResult<Option<Node<'mcx>>> {
    let rte = nsitem.rte();
    let attnum = scanRTEForColumn(pstate, rte, nsitem.p_names, colname, location, 0, None)?;

    if attnum == InvalidAttrNumber {
        return Ok(None);
    }

    for (kind, what) in [
        (ParseExprKind::EXPR_KIND_CHECK_CONSTRAINT, SysColContext::CheckConstraint),
        (ParseExprKind::EXPR_KIND_GENERATED_COLUMN, SysColContext::GeneratedColumn),
        (ParseExprKind::EXPR_KIND_MERGE_WHEN, SysColContext::MergeWhen),
    ] {
        if pstate.p_expr_kind == kind
            && (attnum as i32) < InvalidAttrNumber as i32
            && attnum as i32 != TableOidAttributeNumber
        {
            return Err(bad_system_column_context(pstate, colname, location, what));
        }
    }

    let mut var = if attnum > InvalidAttrNumber {
        let nscol = &nsitem.p_nscolumns[attnum as usize - 1];
        if nscol.p_varno == 0 {
            return Err(dropped_column(nsitem, colname));
        }
        Var {
            varno: nscol.p_varno as i32,
            varattno: nscol.p_varattno,
            vartype: nscol.p_vartype,
            vartypmod: nscol.p_vartypmod,
            varcollid: nscol.p_varcollid,
            varnullingrels: types_nodes::Bitmapset::empty(),
            varlevelsup: sublevels_up as Index,
            varreturningtype: nsitem.p_returning_type,
            varnosyn: nscol.p_varnosyn,
            varattnosyn: nscol.p_varattnosyn,
            location,
        }
    } else {
        let sysatt = catalog_heap::SystemAttributeDefinition(attnum);
        Var {
            varno: nsitem.p_rtindex,
            varattno: attnum,
            vartype: sysatt.atttypid,
            vartypmod: sysatt.atttypmod,
            varcollid: sysatt.attcollation,
            varnullingrels: types_nodes::Bitmapset::empty(),
            varlevelsup: sublevels_up as Index,
            varreturningtype: nsitem.p_returning_type,
            varnosyn: nsitem.p_rtindex as Index,
            varattnosyn: attnum,
            location,
        }
    };
    markNullableIfNeeded(mcx, pstate, &mut var)?;
    markVarForSelectPriv(mcx, pstate, &var)?;
    Node::mk(mcx, var).map(Some)
}

#[allow(clippy::too_many_arguments)]
fn scanRTEForColumn<'mcx>(
    pstate: &ParseState<'_, '_>,
    rte: &'mcx RangeTblEntry<'mcx>,
    eref: &Alias<'_>,
    colname: &str,
    location: ParseLoc,
    fuzzy_rte_penalty: i32,
    mut fuzzystate: Option<(Mcx<'_>, &mut FuzzyAttrMatchState<'mcx>)>,
) -> PgResult<AttrNumber> {
    let mut result = InvalidAttrNumber;
    let mut attnum: AttrNumber = 0;

    for c in &eref.colnames {
        let attcolname = c.as_string().expect("eref colnames are String nodes").sval;
        attnum += 1;
        if attcolname == colname {
            if result != InvalidAttrNumber {
                return Err(ambiguous_column_ref(pstate, colname, location));
            }
            result = attnum;
        }
        if let Some((mcx, ref mut state)) = fuzzystate {
            updateFuzzyAttrMatchState(
                mcx,
                fuzzy_rte_penalty,
                state,
                rte,
                attcolname,
                colname,
                attnum,
            )?;
        }
    }

    if result != InvalidAttrNumber {
        return Ok(result);
    }

    if rte.rtekind == RTEKind::RTE_RELATION && rte.relkind != types_rel::RELKIND_COMPOSITE_TYPE {
        let attnum = specialAttNum(colname);
        if attnum != InvalidAttrNumber
            && syscache_seams::search_syscache_exists_attnum::call(rte.relid, attnum)?
        {
            result = attnum;
        }
    }

    Ok(result)
}

fn specialAttNum(attname: &str) -> AttrNumber {
    match catalog_heap::SystemAttributeByName(attname) {
        Some(sysatt) => sysatt.attnum as AttrNumber,
        None => InvalidAttrNumber,
    }
}

pub fn colNameToVar<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    colname: &str,
    localonly: bool,
    location: ParseLoc,
) -> PgResult<Option<Node<'mcx>>> {
    let mut result: Option<Node<'mcx>> = None;
    let mut sublevels_up = 0;
    let orig_pstate = pstate;

    let mut ps = Some(pstate);
    while let Some(p) = ps {
        for nsitem in p.p_namespace.iter().copied() {
            if !nsitem.p_cols_visible {
                continue;
            }
            if nsitem.p_lateral_only.get() && !p.p_lateral_active {
                continue;
            }
            let newresult =
                scanNSItemForColumn(mcx, orig_pstate, nsitem, sublevels_up, colname, location)?;
            if let Some(newresult) = newresult {
                if result.is_some() {
                    return Err(ambiguous_column_ref(p, colname, location));
                }
                check_lateral_ref_ok(p, nsitem, location)?;
                result = Some(newresult);
            }
        }
        if result.is_some() || localonly {
            break;
        }
        ps = p.parentParseState;
        sublevels_up += 1;
    }
    Ok(result)
}

struct FuzzyAttrMatchState<'mcx> {
    distance: i32,
    rfirst: Option<&'mcx RangeTblEntry<'mcx>>,
    first: AttrNumber,
    rsecond: Option<&'mcx RangeTblEntry<'mcx>>,
    second: AttrNumber,
    rexact1: Option<&'mcx RangeTblEntry<'mcx>>,
    rexact2: Option<&'mcx RangeTblEntry<'mcx>>,
}

fn updateFuzzyAttrMatchState<'mcx>(
    mcx: Mcx<'_>,
    fuzzy_rte_penalty: i32,
    fuzzystate: &mut FuzzyAttrMatchState<'mcx>,
    rte: &'mcx RangeTblEntry<'mcx>,
    actual: &str,
    matched: &str,
    attnum: AttrNumber,
) -> PgResult<()> {
    if fuzzy_rte_penalty > fuzzystate.distance {
        return Ok(());
    }
    // Dropped columns appear as empty eref names; reject them outright.
    if actual.is_empty() {
        return Ok(());
    }

    let matchlen = matched.len() as i32;
    let mut columndistance = varlena::levenshtein::varstr_levenshtein_less_equal(
        mcx,
        actual.as_bytes(),
        matched.as_bytes(),
        1,
        1,
        1,
        fuzzystate.distance + 1 - fuzzy_rte_penalty,
        true,
    )?;

    // More than half the characters different is not a useful suggestion.
    if columndistance > matchlen / 2 {
        return Ok(());
    }

    columndistance += fuzzy_rte_penalty;

    if columndistance < fuzzystate.distance {
        fuzzystate.distance = columndistance;
        fuzzystate.rfirst = Some(rte);
        fuzzystate.first = attnum;
        fuzzystate.rsecond = None;
    } else if columndistance == fuzzystate.distance {
        if fuzzystate.rsecond.is_some() {
            // Three at the same distance: too loose a bar to hint from.
            fuzzystate.rfirst = None;
            fuzzystate.rsecond = None;
        } else if fuzzystate.rfirst.is_some() {
            fuzzystate.rsecond = Some(rte);
            fuzzystate.second = attnum;
        }
    }
    Ok(())
}

fn searchRangeTableForCol<'p, 'mcx>(
    mcx: Mcx<'_>,
    pstate: &'p ParseState<'p, 'mcx>,
    alias: Option<&str>,
    colname: &str,
    location: ParseLoc,
) -> PgResult<FuzzyAttrMatchState<'mcx>> {
    let orig_pstate = pstate;
    let mut state = FuzzyAttrMatchState {
        distance: MAX_FUZZY_DISTANCE + 1,
        rfirst: None,
        first: InvalidAttrNumber,
        rsecond: None,
        second: InvalidAttrNumber,
        rexact1: None,
        rexact2: None,
    };

    let mut ps = Some(pstate);
    while let Some(p) = ps {
        for rte_node in &p.p_rtable {
            let rte = rte_node.as_range_tbl_entry().expect("rtable holds RangeTblEntry");
            if rte.rtekind == RTEKind::RTE_JOIN {
                continue;
            }
            let eref = rte.eref.expect("analyzed RTE always has eref");
            let fuzzy_rte_penalty = match alias {
                Some(alias) => varlena::levenshtein::varstr_levenshtein_less_equal(
                    mcx,
                    alias.as_bytes(),
                    eref.aliasname.unwrap_or("").as_bytes(),
                    1,
                    1,
                    1,
                    MAX_FUZZY_DISTANCE + 1,
                    true,
                )?,
                None => 0,
            };
            let attnum = scanRTEForColumn(
                orig_pstate,
                rte,
                eref,
                colname,
                location,
                fuzzy_rte_penalty,
                Some((mcx, &mut state)),
            )?;
            if attnum != InvalidAttrNumber && fuzzy_rte_penalty == 0 {
                if state.rexact1.is_none() {
                    state.rexact1 = Some(rte);
                } else {
                    state.rexact2 = Some(rte);
                }
            }
        }
        ps = p.parentParseState;
    }
    Ok(state)
}

pub fn markNullableIfNeeded<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    var: &mut Var<'mcx>,
) -> PgResult<()> {
    let rtindex = var.varno;
    let mut p = pstate;
    for _ in 0..var.varlevelsup {
        p = p.parentParseState.expect("varlevelsup exceeds pstate depth");
    }
    if rtindex > 0 && (rtindex as usize) <= p.p_nullingrels.len() {
        let relids = &p.p_nullingrels[rtindex as usize - 1];
        if !relids.is_empty() {
            var.varnullingrels.add_members(mcx, relids)?;
        }
    }
    Ok(())
}

fn markRTEForSelectPriv(
    mcx: Mcx<'_>,
    pstate: &ParseState<'_, '_>,
    rtindex: i32,
    col: AttrNumber,
) -> PgResult<()> {
    let rte = pstate
        .p_rtable
        .nth(rtindex as usize - 1)
        .as_range_tbl_entry()
        .expect("rtable holds RangeTblEntry");
    match rte.rtekind {
        RTEKind::RTE_RELATION => {
            let perminfo = getRTEPermissionInfo(&pstate.p_rteperminfos, rte)?;
            // SAFETY: perminfo nodes are read only through transient as_*
            // lookups; no derived reference is live across this call.
            unsafe {
                perminfo.with_mut::<RTEPermissionInfo, _>(|p| {
                    p.requiredPerms |= ACL_SELECT;
                    p.selectedCols
                        .add_member(mcx, col as i32 - FirstLowInvalidHeapAttributeNumber)
                })
            }
            .expect("perminfoindex resolves to RTEPermissionInfo")?;
        }
        // A whole-row join reference propagates to both inputs; merged USING
        // columns need nothing (the join qual already marked the inputs).
        RTEKind::RTE_JOIN => {
            if col == InvalidAttrNumber {
                let j = pstate
                    .p_joinexprs
                    .get(rtindex as usize - 1)
                    .copied()
                    .flatten()
                    .and_then(|n| n.as_join_expr())
                    .expect("could not find JoinExpr for whole-row reference");
                for arg in [j.larg, j.rarg] {
                    let varno = match arg.node_tag() {
                        NodeTag::T_RangeTblRef => arg.as_range_tbl_ref().unwrap().rtindex,
                        NodeTag::T_JoinExpr => arg.as_join_expr().unwrap().rtindex,
                        other => panic!("unrecognized node type: {other:?}"),
                    };
                    markRTEForSelectPriv(mcx, pstate, varno, InvalidAttrNumber)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

pub fn markVarForSelectPriv(
    mcx: Mcx<'_>,
    pstate: &ParseState<'_, '_>,
    var: &Var<'_>,
) -> PgResult<()> {
    let mut p = pstate;
    for _ in 0..var.varlevelsup {
        p = p.parentParseState.expect("varlevelsup exceeds pstate depth");
    }
    markRTEForSelectPriv(mcx, p, var.varno, var.varattno)
}

fn buildRelationAliases<'mcx>(
    mcx: Mcx<'mcx>,
    tupdesc: &TupleDescData<'mcx>,
    alias: Option<&'mcx Alias<'mcx>>,
    eref_aliasname: &'mcx str,
) -> PgResult<(&'mcx Alias<'mcx>, Option<&'mcx Alias<'mcx>>)> {
    let maxattrs = tupdesc.natts as usize;
    let user_colnames: &[Node<'mcx>] = alias.map_or(&[], |a| a.colnames.as_slice());
    let numaliases = user_colnames.len();
    let mut next_alias = 0usize;
    let mut numdropped = 0usize;

    let mut eref_colnames = NodeList::nil();
    // C rebuilds alias->colnames in place (empty strings inserted for dropped
    // columns); the raw-tree Alias is immutable here, so a rebuilt copy goes
    // on the RTE instead — content matches C's post-rebuild state.
    let mut rebuilt_alias_colnames = NodeList::nil();

    for varattno in 0..maxattrs {
        let attr = tupdesc.attr(varattno);
        let attrname: Node<'mcx>;
        if attr.attisdropped {
            attrname = Node::mk_string(mcx, "")?;
            if next_alias < numaliases {
                rebuilt_alias_colnames.lappend(mcx, attrname)?;
            }
            numdropped += 1;
        } else if next_alias < numaliases {
            attrname = user_colnames[next_alias];
            next_alias += 1;
            rebuilt_alias_colnames.lappend(mcx, attrname)?;
        } else {
            let name = core::str::from_utf8(attr.attname.name_str())
                .expect("catalog attnames are UTF-8");
            attrname = Node::mk_string(mcx, str_in(mcx, name)?)?;
        }
        eref_colnames.lappend(mcx, attrname)?;
    }

    if next_alias < numaliases {
        return Err(too_many_aliases(eref_aliasname, maxattrs - numdropped, numaliases));
    }

    let eref = Node::mk_mut(mcx, Alias { aliasname: Some(eref_aliasname), colnames: eref_colnames })?
        .seal_ref();
    let rebuilt_alias = match alias {
        Some(a) => Some(
            Node::mk_mut(mcx, Alias { aliasname: a.aliasname, colnames: rebuilt_alias_colnames })?
                .seal_ref() as &'mcx Alias<'mcx>,
        ),
        None => None,
    };
    Ok((eref, rebuilt_alias))
}

fn buildNSItemFromTupleDesc<'mcx>(
    mcx: Mcx<'mcx>,
    rte_node: Node<'mcx>,
    rtindex: i32,
    perminfo: Option<Node<'mcx>>,
    tupdesc: &TupleDescData<'mcx>,
) -> PgResult<ParseNamespaceItem<'mcx>> {
    let rte = rte_node.as_range_tbl_entry().expect("p_rte is RangeTblEntry");
    let maxattrs = tupdesc.natts as usize;
    debug_assert_eq!(maxattrs, rte.eref.expect("rte has eref").colnames.len());

    let mut nscolumns: PgVec<'mcx, ParseNamespaceColumn> =
        mcx::vec_with_capacity_in(mcx, maxattrs)?;
    for varattno in 0..maxattrs {
        let attr = tupdesc.attr(varattno);
        if attr.attisdropped {
            nscolumns.push(ParseNamespaceColumn::default());
            continue;
        }
        nscolumns.push(ParseNamespaceColumn {
            p_varno: rtindex as Index,
            p_varattno: varattno as AttrNumber + 1,
            p_vartype: attr.atttypid,
            p_vartypmod: attr.atttypmod,
            p_varcollid: attr.attcollation,
            p_varreturningtype: VarReturningType::VAR_RETURNING_DEFAULT,
            p_varnosyn: rtindex as Index,
            p_varattnosyn: varattno as AttrNumber + 1,
            p_dontexpand: false,
        });
    }

    Ok(ParseNamespaceItem {
        p_names: rte.eref.expect("rte has eref"),
        p_rte: rte_node,
        p_rtindex: rtindex,
        p_perminfo: perminfo,
        p_nscolumns: nscolumns.leak(),
        p_rel_visible: true,
        p_cols_visible: true,
        p_lateral_only: core::cell::Cell::new(false),
        p_lateral_ok: core::cell::Cell::new(true),
        p_returning_type: VarReturningType::VAR_RETURNING_DEFAULT,
    })
}

pub fn parserOpenTable<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    relation: &types_nodes::RangeVar<'_>,
    lockmode: LOCKMODE,
) -> PgResult<Relation<'mcx>> {
    let rv = to_rel_vocab(relation);
    match table::table_openrv_extended(mcx, &rv, lockmode, true)? {
        Some(rel) => Ok(rel),
        None => {
            let relname = relation.relname.expect("grammar always sets relname");
            if relation.schemaname.is_none() && isFutureCTE(pstate, relname) {
                return Err(Box::new(
                    elog::ereport(ERROR)
                        .errcode(ERRCODE_UNDEFINED_TABLE)
                        .errmsg(format!("relation \"{relname}\" does not exist"))
                        .errdetail(format!(
                            "There is a WITH item named \"{relname}\", but it cannot be \
                             referenced from this part of the query."
                        ))
                        .errhint(
                            "Use WITH RECURSIVE, or re-order the WITH items to remove \
                             forward references.",
                        )
                        .errposition(errpos(pstate, relation.location))
                        .into_error()
                        .with_error_location(loc("parserOpenTable")),
                ));
            }
            Err(undefined_table(pstate, relation))
        }
    }
}

pub fn isLockedRefname(pstate: &ParseState<'_, '_>, refname: Option<&str>) -> bool {
    if pstate.p_locked_from_parent {
        return true;
    }
    for lc_node in &pstate.p_locking_clause {
        let lc = lc_node.as_locking_clause().expect("p_locking_clause cell");
        if lc.lockedRels.is_nil() {
            return true;
        }
        for rel_node in &lc.lockedRels {
            let thisrel = rel_node.as_range_var().expect("lockedRels cell");
            if refname == thisrel.relname {
                return true;
            }
        }
    }
    false
}

/// `get_parse_rowmark` (parse_relation.c); returns the node handle so
/// applyLockingClause can merge strengths in place.
pub fn get_parse_rowmark<'mcx>(
    qry: &types_nodes::Query<'mcx>,
    rtindex: Index,
) -> Option<Node<'mcx>> {
    qry.rowMarks
        .iter()
        .find(|rc| rc.as_row_mark_clause().expect("rowMarks cell").rti == rtindex)
}

pub fn addRangeTableEntry<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    relation: &types_nodes::RangeVar<'mcx>,
    alias: Option<&'mcx Alias<'mcx>>,
    inh: bool,
    inFromCl: bool,
) -> PgResult<&'mcx mut ParseNamespaceItem<'mcx>> {
    let refname = alias
        .and_then(|a| a.aliasname)
        .or(relation.relname)
        .expect("grammar always sets relname");

    let lockmode =
        if isLockedRefname(pstate, Some(refname)) { RowShareLock } else { AccessShareLock };

    let rel = parserOpenTable(mcx, pstate, relation, lockmode)?;

    let (eref, rebuilt_alias) =
        buildRelationAliases(mcx, &rel.rd_att, alias, str_in(mcx, refname)?)?;

    let mut rte = RangeTblEntry {
        rtekind: RTEKind::RTE_RELATION,
        alias: rebuilt_alias,
        relid: rel.rd_id,
        inh,
        relkind: rel.rd_rel.relkind,
        rellockmode: lockmode,
        eref: Some(eref),
        lateral: false,
        inFromCl,
        ..Default::default()
    };

    let perminfo = addRTEPermissionInfo(mcx, &mut pstate.p_rteperminfos, &mut rte)?;
    // SAFETY: the perminfo node was created just above; no derived reference exists.
    unsafe { perminfo.with_mut::<RTEPermissionInfo, _>(|p| p.requiredPerms = ACL_SELECT) }
        .expect("node built as RTEPermissionInfo");

    let rte_node = Node::mk(mcx, rte)?;
    pstate.p_rtable.lappend(mcx, rte_node)?;
    let rtindex = pstate.p_rtable.len() as i32;

    let nsitem = buildNSItemFromTupleDesc(
        mcx,
        rte_node,
        rtindex,
        Some(perminfo),
        &rel.rd_att,
    )?;

    table::table_close(rel, NoLock)?;

    Ok(mcx::leak_in(mcx::alloc_in(mcx, nsitem)?))
}

pub fn addRangeTableEntryForRelation<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    rel: &Relation<'mcx>,
    lockmode: LOCKMODE,
    alias: Option<&'mcx Alias<'mcx>>,
    inh: bool,
    inFromCl: bool,
) -> PgResult<&'mcx mut ParseNamespaceItem<'mcx>> {
    let refname = match alias.and_then(|a| a.aliasname) {
        Some(name) => name,
        // Names are single-byte-safe UTF-8 identifiers copied from the query.
        None => str_in(
            mcx,
            core::str::from_utf8(rel.rd_rel.relname.name_str()).expect("relname is UTF-8"),
        )?,
    };

    let (eref, rebuilt_alias) = buildRelationAliases(mcx, &rel.rd_att, alias, refname)?;

    let mut rte = RangeTblEntry {
        rtekind: RTEKind::RTE_RELATION,
        alias: rebuilt_alias,
        relid: rel.rd_id,
        inh,
        relkind: rel.rd_rel.relkind,
        rellockmode: lockmode,
        eref: Some(eref),
        lateral: false,
        inFromCl,
        ..Default::default()
    };
    let perminfo = addRTEPermissionInfo(mcx, &mut pstate.p_rteperminfos, &mut rte)?;
    // SAFETY: the perminfo node was created just above; no derived reference exists.
    unsafe { perminfo.with_mut::<RTEPermissionInfo, _>(|p| p.requiredPerms = ACL_SELECT) }
        .expect("node built as RTEPermissionInfo");

    let rte_node = Node::mk(mcx, rte)?;
    pstate.p_rtable.lappend(mcx, rte_node)?;
    let rtindex = pstate.p_rtable.len() as i32;

    let nsitem = buildNSItemFromTupleDesc(
        mcx,
        rte_node,
        rtindex,
        Some(perminfo),
        &rel.rd_att,
    )?;
    Ok(mcx::leak_in(mcx::alloc_in(mcx, nsitem)?))
}

fn chooseScalarFunctionAlias<'mcx>(
    mcx: Mcx<'mcx>,
    funcexpr: Node<'mcx>,
    funcname: &'mcx str,
    alias: Option<&'mcx Alias<'mcx>>,
    nfuncs: usize,
) -> PgResult<&'mcx str> {
    if let Some(fe) = funcexpr.as_func_expr() {
        if let Some(pname) = funcapi::get_func_result_name(mcx, fe.funcid)? {
            return Ok(pname);
        }
    }
    if nfuncs == 1 {
        if let Some(name) = alias.and_then(|a| a.aliasname) {
            return Ok(name);
        }
    }
    Ok(funcname)
}

// GetColumnDefCollation (parse_type.c).
fn GetColumnDefCollation(
    pstate: &ParseState<'_, '_>,
    coldef: &types_nodes::rawnodes::ColumnDef<'_>,
    type_oid: Oid,
) -> PgResult<Oid> {
    let typcollation = syscache_seams::lookup_pg_type_shape::call(type_oid)?
        .expect("pg_type row vanished")
        .typcollation;
    let result = if let Some(cc) = coldef.collClause {
        let cc = cc.as_collate_clause().expect("CollateClause");
        catalog_namespace::get_collation_oid_list(&cc.collname, false)?
    } else if coldef.collOid != InvalidOid {
        coldef.collOid
    } else {
        typcollation
    };
    if result != InvalidOid && typcollation == InvalidOid {
        let typename = format_type::format_type_be(type_oid)
            .unwrap_or_else(|_| format!("type {type_oid}"));
        return Err(Box::new(
            elog::ereport(ERROR)
                .errcode(types_error::ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!("collations are not supported by type {typename}"))
                .errposition(errpos(pstate, coldef.location))
                .into_error()
                .with_error_location(loc("GetColumnDefCollation")),
        ));
    }
    Ok(result)
}

#[track_caller]
#[cold]
#[inline(never)]
fn coldeflist_error(
    pstate: &ParseState<'_, '_>,
    code: types_error::SqlState,
    msg: String,
    location: ParseLoc,
) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(code)
            .errmsg(msg)
            .errposition(errpos(pstate, location))
            .into_error()
            .with_error_location(loc("addRangeTableEntryForFunction")),
    )
}

pub fn addRangeTableEntryForFunction<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    funcnames: &[&'mcx str],
    funcexprs: NodeList<'mcx>,
    coldeflists: &[&'mcx NodeList<'mcx>],
    rangefunc: &'mcx types_nodes::RangeFunction<'mcx>,
    lateral: bool,
    inFromCl: bool,
) -> PgResult<&'mcx mut ParseNamespaceItem<'mcx>> {
    use types_error::{ERRCODE_SYNTAX_ERROR, ERRCODE_TOO_MANY_COLUMNS};
    use types_tuple::htup::{MaxHeapAttributeNumber, MaxTupleAttributeNumber};

    let alias = rangefunc.alias;
    let nfuncs = funcexprs.len();
    let aliasname = alias.and_then(|a| a.aliasname).unwrap_or(funcnames[0]);

    let mut functions = NodeList::nil();
    let mut functupdescs: PgVec<'mcx, TupleDescData<'mcx>> = PgVec::new_in(mcx);
    let mut totalatts: i32 = 0;

    for (funcno, funcexpr) in funcexprs.iter().enumerate() {
        let funcname = funcnames[funcno];
        let coldeflist = coldeflists[funcno];

        let resolved = funcapi::get_expr_result_type(mcx, Some(funcexpr))?;

        if !coldeflist.is_nil() {
            match resolved.class {
                funcapi::TypeFuncClass::Record => {}
                funcapi::TypeFuncClass::Composite
                | funcapi::TypeFuncClass::CompositeDomain => {
                    let msg = if nodes_core::node_funcs::expr_type(funcexpr)
                        == types_core::catalog::RECORDOID
                    {
                        "a column definition list is redundant for a function with OUT \
                         parameters"
                    } else {
                        "a column definition list is redundant for a function returning a \
                         named composite type"
                    };
                    return Err(coldeflist_error(
                        pstate,
                        ERRCODE_SYNTAX_ERROR,
                        msg.into(),
                        nodes_core::node_funcs::expr_location_list(coldeflist),
                    ));
                }
                _ => {
                    return Err(coldeflist_error(
                        pstate,
                        ERRCODE_SYNTAX_ERROR,
                        "a column definition list is only allowed for functions returning \
                         \"record\""
                            .into(),
                        nodes_core::node_funcs::expr_location_list(coldeflist),
                    ));
                }
            }
        } else if matches!(resolved.class, funcapi::TypeFuncClass::Record) {
            return Err(coldeflist_error(
                pstate,
                ERRCODE_SYNTAX_ERROR,
                "a column definition list is required for functions returning \"record\""
                    .into(),
                nodes_core::node_funcs::expr_location(funcexpr),
            ));
        }

        let mut funccolnames = NodeList::nil();
        let mut funccoltypes = types_nodes::OidList::nil();
        let mut funccoltypmods = types_nodes::IntList::nil();
        let mut funccolcollations = types_nodes::OidList::nil();

        let tupdesc = match resolved.class {
            funcapi::TypeFuncClass::Composite | funcapi::TypeFuncClass::CompositeDomain => {
                resolved.result_tuple_desc.unwrap_or_else(|| {
                    panic!(
                        "addRangeTableEntryForFunction (parse_relation.c): composite \
                         result without a tupdesc"
                    )
                })
            }
            funcapi::TypeFuncClass::Scalar => {
                let colname =
                    chooseScalarFunctionAlias(mcx, funcexpr, funcname, alias, nfuncs)?;
                let typmod = nodes_core::node_funcs::expr_typmod(funcexpr);
                let collation = nodes_core::node_funcs::expr_collation(funcexpr);
                let mut d = tupdesc::CreateTemplateTupleDesc(mcx, 1)?;
                tupdesc::TupleDescInitEntry(
                    &mut d,
                    1,
                    Some(colname),
                    resolved.result_type_id,
                    typmod,
                    0,
                )?;
                tupdesc::TupleDescInitEntryCollation(&mut d, 1, collation);
                d
            }
            funcapi::TypeFuncClass::Record => {
                if coldeflist.len() as i32 > MaxHeapAttributeNumber {
                    return Err(coldeflist_error(
                        pstate,
                        ERRCODE_TOO_MANY_COLUMNS,
                        format!(
                            "column definition lists can have at most \
                             {MaxHeapAttributeNumber} entries"
                        ),
                        nodes_core::node_funcs::expr_location_list(coldeflist),
                    ));
                }
                let mut d = tupdesc::CreateTemplateTupleDesc(mcx, coldeflist.len() as i32)?;
                for (i, col) in coldeflist.iter().enumerate() {
                    let n = col
                        .as_variant::<types_nodes::rawnodes::ColumnDef>()
                        .expect("coldeflist cell is ColumnDef");
                    let attrname = n.colname.expect("TableFuncElement always names");
                    let tn = n
                        .typeName
                        .expect("TableFuncElement always types")
                        .as_type_name()
                        .expect("TypeName");
                    if tn.setof {
                        return Err(coldeflist_error(
                            pstate,
                            types_error::ERRCODE_INVALID_TABLE_DEFINITION,
                            format!("column \"{attrname}\" cannot be declared SETOF"),
                            n.location,
                        ));
                    }
                    // C typenameTypeIdAndMod has no typtype gate; record and
                    // record[] pass here, checked below by
                    // CheckAttributeNamesTypes(CHKATYPE_ANYRECORD).
                    let (attrtype, attrtypmod) = parse_utilcmd_seams::typename_type_id_and_mod_any::call(
                        mcx,
                        Some(pstate),
                        tn,
                    )?;
                    let attrcollation = GetColumnDefCollation(pstate, n, attrtype)?;
                    let attno = (i + 1) as AttrNumber;
                    tupdesc::TupleDescInitEntry(
                        &mut d,
                        attno,
                        Some(attrname),
                        attrtype,
                        attrtypmod,
                        0,
                    )?;
                    tupdesc::TupleDescInitEntryCollation(&mut d, attno, attrcollation);
                    funccolnames
                        .lappend(mcx, Node::mk_string(mcx, str_in(mcx, attrname)?)?)?;
                    funccoltypes.lappend(mcx, attrtype)?;
                    funccoltypmods.lappend(mcx, attrtypmod)?;
                    funccolcollations.lappend(mcx, attrcollation)?;
                }
                catalog_heap::CheckAttributeNamesTypes(
                    mcx,
                    &d,
                    types_rel::RELKIND_COMPOSITE_TYPE,
                    catalog_heap::CHKATYPE_ANYRECORD,
                )?;
                d
            }
            funcapi::TypeFuncClass::Other => {
                return Err(unsupported_function_return_type(
                    pstate,
                    funcname,
                    resolved.result_type_id,
                    node_funcs::expr_location(funcexpr),
                ))
            }
        };

        let rtfunc = Node::mk(
            mcx,
            types_nodes::RangeTblFunction {
                funcexpr: Some(funcexpr),
                funccolcount: tupdesc.natts,
                funccolnames,
                funccoltypes,
                funccoltypmods,
                funccolcollations,
                ..Default::default()
            },
        )?;
        functions.lappend(mcx, rtfunc)?;
        totalatts += tupdesc.natts;
        functupdescs.push(tupdesc);
    }

    let tupdesc = if nfuncs > 1 || rangefunc.ordinality {
        if rangefunc.ordinality {
            totalatts += 1;
        }
        if totalatts > MaxTupleAttributeNumber {
            return Err(coldeflist_error(
                pstate,
                ERRCODE_TOO_MANY_COLUMNS,
                format!(
                    "functions in FROM can return at most {MaxTupleAttributeNumber} columns"
                ),
                nodes_core::node_funcs::expr_location_list(&funcexprs),
            ));
        }
        let mut d = tupdesc::CreateTemplateTupleDesc(mcx, totalatts)?;
        let mut natts: AttrNumber = 0;
        for fd in functupdescs.iter() {
            for j in 1..=fd.natts {
                natts += 1;
                tupdesc::TupleDescCopyEntry(&mut d, natts, fd, j as AttrNumber);
            }
        }
        if rangefunc.ordinality {
            natts += 1;
            tupdesc::TupleDescInitEntry(
                &mut d,
                natts,
                Some("ordinality"),
                types_core::catalog::INT8OID,
                -1,
                0,
            )?;
        }
        debug_assert_eq!(natts as i32, totalatts);
        d
    } else {
        functupdescs.pop().expect("at least one function")
    };

    let (eref, rebuilt_alias) =
        buildRelationAliases(mcx, &tupdesc, alias, str_in(mcx, aliasname)?)?;

    // Functions are never checked for access rights: no addRTEPermissionInfo.
    let rte = RangeTblEntry {
        rtekind: RTEKind::RTE_FUNCTION,
        alias: rebuilt_alias,
        functions,
        funcordinality: rangefunc.ordinality,
        eref: Some(eref),
        lateral,
        inFromCl,
        ..Default::default()
    };
    let rte_node = Node::mk(mcx, rte)?;
    pstate.p_rtable.lappend(mcx, rte_node)?;
    let rtindex = pstate.p_rtable.len() as i32;

    let nsitem = buildNSItemFromTupleDesc(
        mcx,
        rte_node,
        rtindex,
        None,
        &tupdesc,
    )?;
    Ok(mcx::leak_in(mcx::alloc_in(mcx, nsitem)?))
}

pub fn addRangeTableEntryForValues<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    exprs: NodeList<'mcx>,
    coltypes: types_nodes::list::OidList<'mcx>,
    coltypmods: types_nodes::list::IntList<'mcx>,
    colcollations: types_nodes::list::OidList<'mcx>,
    alias: Option<&'mcx Alias<'mcx>>,
    lateral: bool,
    inFromCl: bool,
) -> PgResult<&'mcx mut ParseNamespaceItem<'mcx>> {
    let refname = alias.and_then(|a| a.aliasname).unwrap_or("*VALUES*");
    let numcolumns = exprs.nth(0).as_list().expect("VALUES row is a List").len();
    let numaliases = alias.map(|a| a.colnames.len()).unwrap_or(0);
    if numaliases > numcolumns {
        return Err(too_many_aliases(refname, numcolumns, numaliases));
    }

    let mut eref_colnames = NodeList::nil();
    for i in 0..numcolumns {
        let name = match alias {
            Some(a) if i < numaliases => a.colnames.nth(i),
            _ => Node::mk_string(mcx, str_in(mcx, &format!("column{}", i + 1))?)?,
        };
        eref_colnames.lappend(mcx, name)?;
    }
    let eref = Node::mk_mut(mcx, Alias { aliasname: Some(refname), colnames: eref_colnames })?
        .seal_ref();

    let rte = RangeTblEntry {
        rtekind: RTEKind::RTE_VALUES,
        alias,
        values_lists: exprs,
        coltypes,
        coltypmods,
        colcollations,
        eref: Some(eref),
        lateral,
        inFromCl,
        ..Default::default()
    };
    let rte_node = Node::mk(mcx, rte)?;
    pstate.p_rtable.lappend(mcx, rte_node)?;
    let rtindex = pstate.p_rtable.len() as i32;

    let rte = rte_node.as_range_tbl_entry().expect("just built");
    let mut nscolumns: PgVec<'mcx, ParseNamespaceColumn> =
        mcx::vec_with_capacity_in(mcx, numcolumns)?;
    for varattno in 0..numcolumns {
        nscolumns.push(ParseNamespaceColumn {
            p_varno: rtindex as Index,
            p_varattno: varattno as AttrNumber + 1,
            p_vartype: rte.coltypes.nth(varattno),
            p_vartypmod: rte.coltypmods.nth(varattno),
            p_varcollid: rte.colcollations.nth(varattno),
            p_varreturningtype: VarReturningType::VAR_RETURNING_DEFAULT,
            p_varnosyn: rtindex as Index,
            p_varattnosyn: varattno as AttrNumber + 1,
            p_dontexpand: false,
        });
    }
    let nsitem = ParseNamespaceItem {
        p_names: rte.eref.expect("rte has eref"),
        p_rte: rte_node,
        p_rtindex: rtindex,
        p_perminfo: None,
        p_nscolumns: nscolumns.leak(),
        p_rel_visible: true,
        p_cols_visible: true,
        p_lateral_only: core::cell::Cell::new(false),
        p_lateral_ok: core::cell::Cell::new(true),
        p_returning_type: VarReturningType::VAR_RETURNING_DEFAULT,
    };
    Ok(mcx::leak_in(mcx::alloc_in(mcx, nsitem)?))
}

// C addRangeTableEntryForTableFunc (parse_relation.c).
pub fn addRangeTableEntryForTableFunc<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    tf_node: Node<'mcx>,
    alias: Option<&'mcx Alias<'mcx>>,
    lateral: bool,
    inFromCl: bool,
) -> PgResult<&'mcx mut ParseNamespaceItem<'mcx>> {
    let tf = tf_node.as_table_func().expect("TableFunc node");
    let ncolumns = tf.colnames.len();
    if ncolumns > types_tuple::htup::MaxTupleAttributeNumber as usize {
        return Err(too_many_tablefunc_columns(pstate, tf.location));
    }
    debug_assert_eq!(tf.coltypes.len(), ncolumns);
    debug_assert_eq!(tf.coltypmods.len(), ncolumns);
    debug_assert_eq!(tf.colcollations.len(), ncolumns);

    let refname = alias.and_then(|a| a.aliasname).unwrap_or(
        if tf.functype == types_nodes::primnodes::TableFuncType::TFT_XMLTABLE {
            "xmltable"
        } else {
            "json_table"
        },
    );
    let mut eref_colnames = match alias {
        Some(a) => a.colnames.clone_in(mcx)?,
        None => NodeList::nil(),
    };
    let numaliases = eref_colnames.len();
    for i in numaliases..ncolumns {
        eref_colnames.lappend(mcx, tf.colnames.nth(i))?;
    }
    if numaliases > ncolumns {
        return Err(tablefunc_too_many_aliases(
            if tf.functype == types_nodes::primnodes::TableFuncType::TFT_XMLTABLE {
                "XMLTABLE"
            } else {
                "JSON_TABLE"
            },
            ncolumns,
            numaliases,
        ));
    }
    let eref = Node::mk_mut(mcx, Alias { aliasname: Some(refname), colnames: eref_colnames })?
        .seal_ref();

    let rte = RangeTblEntry {
        rtekind: RTEKind::RTE_TABLEFUNC,
        tablefunc: Some(tf_node),
        coltypes: tf.coltypes.clone_in(mcx)?,
        coltypmods: tf.coltypmods.clone_in(mcx)?,
        colcollations: tf.colcollations.clone_in(mcx)?,
        alias,
        eref: Some(eref),
        lateral,
        inFromCl,
        ..Default::default()
    };
    let rte_node = Node::mk(mcx, rte)?;
    pstate.p_rtable.lappend(mcx, rte_node)?;
    let rtindex = pstate.p_rtable.len() as i32;

    let rte = rte_node.as_range_tbl_entry().expect("just built");
    let mut nscolumns: PgVec<'mcx, ParseNamespaceColumn> =
        mcx::vec_with_capacity_in(mcx, ncolumns)?;
    for varattno in 0..ncolumns {
        nscolumns.push(ParseNamespaceColumn {
            p_varno: rtindex as Index,
            p_varattno: varattno as AttrNumber + 1,
            p_vartype: rte.coltypes.nth(varattno),
            p_vartypmod: rte.coltypmods.nth(varattno),
            p_varcollid: rte.colcollations.nth(varattno),
            p_varreturningtype: VarReturningType::VAR_RETURNING_DEFAULT,
            p_varnosyn: rtindex as Index,
            p_varattnosyn: varattno as AttrNumber + 1,
            p_dontexpand: false,
        });
    }
    let nsitem = ParseNamespaceItem {
        p_names: rte.eref.expect("rte has eref"),
        p_rte: rte_node,
        p_rtindex: rtindex,
        p_perminfo: None,
        p_nscolumns: nscolumns.leak(),
        p_rel_visible: true,
        p_cols_visible: true,
        p_lateral_only: core::cell::Cell::new(false),
        p_lateral_ok: core::cell::Cell::new(true),
        p_returning_type: VarReturningType::VAR_RETURNING_DEFAULT,
    };
    Ok(mcx::leak_in(mcx::alloc_in(mcx, nsitem)?))
}

#[track_caller]
#[cold]
#[inline(never)]
fn cte_without_returning(
    pstate: &ParseState<'_, '_>,
    ctename: &str,
    location: ParseLoc,
) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("WITH query \"{ctename}\" does not have a RETURNING clause"))
            .errposition(errpos(pstate, location))
            .into_error()
            .with_error_location(loc("addRangeTableEntryForCTE")),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn too_many_tablefunc_columns(
    pstate: &ParseState<'_, '_>,
    location: ParseLoc,
) -> Box<PgError> {
    let encoding = mbutils::GetDatabaseEncoding();
    Box::new(
        elog::ereport(ERROR)
            .errcode(types_error::ERRCODE_TOO_MANY_COLUMNS)
            .errmsg(format!(
                "functions in FROM can return at most {} columns",
                types_tuple::htup::MaxTupleAttributeNumber
            ))
            .errposition(parser_small1::parser_errposition(pstate, location, encoding))
            .into_error()
            .with_error_location(loc("addRangeTableEntryForTableFunc")),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn tablefunc_too_many_aliases(func: &str, available: usize, specified: usize) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            .errmsg(format!(
                "{func} function has {available} columns available but {specified} \
                 columns specified"
            ))
            .into_error()
            .with_error_location(loc("addRangeTableEntryForTableFunc")),
    )
}

/// C addRangeTableEntryForSubquery; the caller passes one
/// `(resname, exprType, exprTypmod, exprCollation)` tuple per non-junk
/// tlist entry (the nodeFuncs slice lives in parse_expr).
pub fn addRangeTableEntryForSubquery<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    subquery: &'mcx types_nodes::parsenodes::Query<'mcx>,
    columns: &[(Option<&'mcx str>, Oid, i32, Oid)],
    alias: Option<&'mcx Alias<'mcx>>,
    lateral: bool,
    inFromCl: bool,
) -> PgResult<&'mcx mut ParseNamespaceItem<'mcx>> {
    let (aliasname, mut eref_colnames) = match alias {
        // C: eref = copyObject(alias) — eref grows default colnames below.
        Some(a) => (a.aliasname, a.colnames.clone_in(mcx)?),
        None => (Some("unnamed_subquery"), NodeList::nil()),
    };
    let numaliases = eref_colnames.len();

    let mut nscolumns: PgVec<'mcx, ParseNamespaceColumn> =
        mcx::vec_with_capacity_in(mcx, columns.len())?;
    for (i, &(resname, coltype, coltypmod, colcoll)) in columns.iter().enumerate() {
        if i >= numaliases {
            let name = resname.expect("non-junk subquery tlist entry has resname");
            eref_colnames.lappend(mcx, Node::mk_string(mcx, name)?)?;
        }
        nscolumns.push(ParseNamespaceColumn {
            p_varno: 0,
            p_varattno: i as AttrNumber + 1,
            p_vartype: coltype,
            p_vartypmod: coltypmod,
            p_varcollid: colcoll,
            p_varreturningtype: VarReturningType::VAR_RETURNING_DEFAULT,
            p_varnosyn: 0,
            p_varattnosyn: i as AttrNumber + 1,
            p_dontexpand: false,
        });
    }
    if columns.len() < numaliases {
        return Err(too_many_aliases(aliasname.unwrap_or(""), columns.len(), numaliases));
    }
    let eref = Node::mk_mut(mcx, Alias { aliasname, colnames: eref_colnames })?.seal_ref();

    let rte = RangeTblEntry {
        rtekind: RTEKind::RTE_SUBQUERY,
        subquery: Some(subquery),
        alias,
        eref: Some(eref),
        lateral,
        inFromCl,
        ..Default::default()
    };
    let rte_node = Node::mk(mcx, rte)?;
    pstate.p_rtable.lappend(mcx, rte_node)?;
    let rtindex = pstate.p_rtable.len() as i32;
    for c in nscolumns.iter_mut() {
        c.p_varno = rtindex as Index;
        c.p_varnosyn = rtindex as Index;
    }

    let rte = rte_node.as_range_tbl_entry().expect("just built");
    let nsitem = ParseNamespaceItem {
        p_names: rte.eref.expect("rte has eref"),
        p_rte: rte_node,
        p_rtindex: rtindex,
        p_perminfo: None,
        p_nscolumns: nscolumns.leak(),
        p_rel_visible: alias.is_some(),
        p_cols_visible: true,
        p_lateral_only: core::cell::Cell::new(false),
        p_lateral_ok: core::cell::Cell::new(true),
        p_returning_type: VarReturningType::VAR_RETURNING_DEFAULT,
    };
    Ok(mcx::leak_in(mcx::alloc_in(mcx, nsitem)?))
}

pub fn scanNameSpaceForCTE<'mcx>(
    pstate: &ParseState<'_, 'mcx>,
    refname: &str,
) -> Option<(Node<'mcx>, Index)> {
    let mut ps: Option<&ParseState<'_, 'mcx>> = Some(pstate);
    let mut levelsup: Index = 0;
    while let Some(p) = ps {
        for cte_node in &p.p_ctenamespace {
            let cte = cte_node.as_common_table_expr().expect("ctenamespace cell");
            if cte.ctename == Some(refname) {
                return Some((cte_node, levelsup));
            }
        }
        ps = p.parentParseState;
        levelsup += 1;
    }
    None
}

fn isFutureCTE(pstate: &ParseState<'_, '_>, refname: &str) -> bool {
    let mut ps: Option<&ParseState<'_, '_>> = Some(pstate);
    while let Some(p) = ps {
        for cte_node in &p.p_future_ctes {
            let cte = cte_node.as_common_table_expr().expect("future_ctes cell");
            if cte.ctename == Some(refname) {
                return true;
            }
        }
        ps = p.parentParseState;
    }
    false
}

pub fn GetCTEForRTE<'mcx>(
    pstate: &ParseState<'_, 'mcx>,
    rte: &RangeTblEntry<'mcx>,
    rtelevelsup: i32,
) -> Node<'mcx> {
    debug_assert_eq!(rte.rtekind, RTEKind::RTE_CTE);
    let ctename = rte.ctename.expect("CTE RTE has ctename");
    let mut ps: Option<&ParseState<'_, 'mcx>> = Some(pstate);
    for _ in 0..(rte.ctelevelsup as i32 + rtelevelsup) {
        ps = ps.and_then(|p| p.parentParseState);
    }
    let p = ps.unwrap_or_else(|| panic!("bad levelsup for CTE \"{ctename}\""));
    for cte_node in &p.p_ctenamespace {
        let cte = cte_node.as_common_table_expr().expect("ctenamespace cell");
        if cte.ctename == Some(ctename) {
            return cte_node;
        }
    }
    panic!("could not find CTE \"{ctename}\"")
}

pub fn addRangeTableEntryForCTE<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    cte_node: Node<'mcx>,
    levelsup: Index,
    rv: &types_nodes::RangeVar<'mcx>,
    inFromCl: bool,
) -> PgResult<&'mcx mut ParseNamespaceItem<'mcx>> {
    let cte = cte_node.as_common_table_expr().expect("CTE reference");

    // Self-reference iff the CTE's analysis isn't completed yet.
    let self_reference = cte.ctequery.expect("CTE has query").as_query().is_none();
    debug_assert!(cte.cterecursive || !self_reference);
    if let Some(q) = cte.ctequery.unwrap().as_query() {
        if q.commandType != types_nodes::nodes_enums::CmdType::CMD_SELECT
            && q.returningList.is_nil()
        {
            return Err(cte_without_returning(pstate, cte.ctename.unwrap_or(""), rv.location));
        }
    }

    let alias = rv.alias;
    let refname = alias.and_then(|a| a.aliasname).or(cte.ctename).expect("cte name");
    let (aliasname, mut eref_colnames) = match alias {
        Some(a) => (a.aliasname, a.colnames.clone_in(mcx)?),
        None => (Some(refname), NodeList::nil()),
    };
    let numaliases = eref_colnames.len();
    let numcols = cte.ctecolnames.len();
    for i in numaliases..numcols {
        eref_colnames.lappend(mcx, cte.ctecolnames.nth(i))?;
    }
    if numcols < numaliases {
        return Err(too_many_aliases(refname, numcols, numaliases));
    }
    let mut coltypes = cte.ctecoltypes.clone_in(mcx)?;
    let mut coltypmods = cte.ctecoltypmods.clone_in(mcx)?;
    let mut colcollations = cte.ctecolcollations.clone_in(mcx)?;
    // The SEARCH/CYCLE output columns exist on the RTE before the rewriter
    // adds them to the CTE itself; inside the CTE they are star-invisible.
    let mut n_dontexpand_columns = 0usize;
    if let Some(sc) = cte.search_clause.map(|n| n.as_cte_search_clause().expect("search clause")) {
        eref_colnames
            .lappend(mcx, Node::mk_string(mcx, sc.search_seq_column.expect("SET column"))?)?;
        coltypes.lappend(
            mcx,
            if sc.search_breadth_first { RECORDOID } else { RECORDARRAYOID },
        )?;
        coltypmods.lappend(mcx, -1)?;
        colcollations.lappend(mcx, InvalidOid)?;
        n_dontexpand_columns += 1;
    }
    if let Some(cc) = cte.cycle_clause.map(|n| n.as_cte_cycle_clause().expect("cycle clause")) {
        eref_colnames
            .lappend(mcx, Node::mk_string(mcx, cc.cycle_mark_column.expect("SET column"))?)?;
        coltypes.lappend(mcx, cc.cycle_mark_type)?;
        coltypmods.lappend(mcx, cc.cycle_mark_typmod)?;
        colcollations.lappend(mcx, cc.cycle_mark_collation)?;
        eref_colnames
            .lappend(mcx, Node::mk_string(mcx, cc.cycle_path_column.expect("USING column"))?)?;
        coltypes.lappend(mcx, RECORDARRAYOID)?;
        coltypmods.lappend(mcx, -1)?;
        colcollations.lappend(mcx, InvalidOid)?;
        n_dontexpand_columns += 2;
    }
    let eref = Node::mk_mut(mcx, Alias { aliasname, colnames: eref_colnames })?.seal_ref();

    let rte = RangeTblEntry {
        rtekind: RTEKind::RTE_CTE,
        ctename: cte.ctename,
        ctelevelsup: levelsup,
        self_reference,
        coltypes,
        coltypmods,
        colcollations,
        alias,
        eref: Some(eref),
        lateral: false,
        inFromCl,
        ..Default::default()
    };
    if !self_reference {
        // SAFETY: shared CommonTableExpr node mutated during analysis; no live
        // derived refs (the `cte` borrow above is dead past the field reads).
        unsafe {
            cte_node.with_mut::<types_nodes::parsenodes::CommonTableExpr, _>(|c| c.cterefcount += 1)
        };
    }

    let rte_node = Node::mk(mcx, rte)?;
    pstate.p_rtable.lappend(mcx, rte_node)?;
    let rtindex = pstate.p_rtable.len() as i32;

    let rte = rte_node.as_range_tbl_entry().expect("just built");
    let numcolumns = rte.coltypes.len();
    let mut nscolumns: PgVec<'mcx, ParseNamespaceColumn> =
        mcx::vec_with_capacity_in(mcx, numcolumns)?;
    for varattno in 0..numcolumns {
        nscolumns.push(ParseNamespaceColumn {
            p_varno: rtindex as Index,
            p_varattno: varattno as AttrNumber + 1,
            p_vartype: rte.coltypes.nth(varattno),
            p_vartypmod: rte.coltypmods.nth(varattno),
            p_varcollid: rte.colcollations.nth(varattno),
            p_varreturningtype: VarReturningType::VAR_RETURNING_DEFAULT,
            p_varnosyn: rtindex as Index,
            p_varattnosyn: varattno as AttrNumber + 1,
            p_dontexpand: levelsup > 0 && varattno >= numcolumns - n_dontexpand_columns,
        });
    }
    let nsitem = ParseNamespaceItem {
        p_names: rte.eref.expect("rte has eref"),
        p_rte: rte_node,
        p_rtindex: rtindex,
        p_perminfo: None,
        p_nscolumns: nscolumns.leak(),
        p_rel_visible: true,
        p_cols_visible: true,
        p_lateral_only: core::cell::Cell::new(false),
        p_lateral_ok: core::cell::Cell::new(true),
        p_returning_type: VarReturningType::VAR_RETURNING_DEFAULT,
    };
    Ok(mcx::leak_in(mcx::alloc_in(mcx, nsitem)?))
}

// ENRs are never checked for access rights: no addRTEPermissionInfo. The
// relid records a dependency for plan invalidation, never a lockable target.
pub fn addRangeTableEntryForENR<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    rv: &types_nodes::RangeVar<'mcx>,
    inFromCl: bool,
) -> PgResult<&'mcx mut ParseNamespaceItem<'mcx>> {
    let relname = rv.relname.expect("grammar always sets relname");
    let alias = rv.alias;
    let refname = alias.and_then(|a| a.aliasname).unwrap_or(relname);

    let enrmd = parser_small1::get_visible_ENR(&*pstate, relname)
        .expect("caller checked name_matches_visible_ENR");
    debug_assert!(enrmd.enrtype == queryenvironment::ENR_NAMED_TUPLESTORE);
    let tupdesc = queryenvironment::ENRMetadataGetTupDesc(mcx, enrmd)?;

    let (eref, rebuilt_alias) =
        buildRelationAliases(mcx, &tupdesc, alias, str_in(mcx, refname)?)?;

    let mut coltypes = types_nodes::list::OidList::nil();
    let mut coltypmods = types_nodes::list::IntList::nil();
    let mut colcollations = types_nodes::list::OidList::nil();
    for attno in 0..tupdesc.natts as usize {
        let att = tupdesc.attr(attno);
        if att.attisdropped {
            coltypes.lappend(mcx, InvalidOid)?;
            coltypmods.lappend(mcx, 0)?;
            colcollations.lappend(mcx, InvalidOid)?;
        } else {
            assert!(
                OidIsValid(att.atttypid),
                "atttypid is invalid for non-dropped column in \"{relname}\""
            );
            coltypes.lappend(mcx, att.atttypid)?;
            coltypmods.lappend(mcx, att.atttypmod)?;
            colcollations.lappend(mcx, att.attcollation)?;
        }
    }

    let rte = RangeTblEntry {
        rtekind: RTEKind::RTE_NAMEDTUPLESTORE,
        alias: rebuilt_alias,
        relid: enrmd.reliddesc,
        enrname: Some(str_in(mcx, enrmd.name.as_str())?),
        enrtuples: enrmd.enrtuples,
        coltypes,
        coltypmods,
        colcollations,
        eref: Some(eref),
        lateral: false,
        inFromCl,
        ..Default::default()
    };
    let rte_node = Node::mk(mcx, rte)?;
    pstate.p_rtable.lappend(mcx, rte_node)?;
    let rtindex = pstate.p_rtable.len() as i32;

    let nsitem = buildNSItemFromTupleDesc(
        mcx,
        rte_node,
        rtindex,
        None,
        &tupdesc,
    )?;
    Ok(mcx::leak_in(mcx::alloc_in(mcx, nsitem)?))
}

// addRangeTableEntryForGroup (parse_relation.c): the RTE_GROUP entry built
// from the grouping TargetEntries. Not added to the joinlist or namespace —
// caller (parse_agg's RTE_GROUP rewrite) does that if appropriate. The
// grouping step is never checked for access rights (no perminfo).
pub fn addRangeTableEntryForGroup<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    groupClauses: &NodeList<'mcx>,
) -> PgResult<&'mcx mut ParseNamespaceItem<'mcx>> {
    let mut eref_colnames = NodeList::nil();
    let mut groupexprs = NodeList::nil();
    let mut nscolumns: PgVec<'mcx, ParseNamespaceColumn> =
        mcx::vec_with_capacity_in(mcx, groupClauses.len())?;
    for (i, te_node) in groupClauses.iter().enumerate() {
        let te = te_node.as_target_entry().expect("groupClauses are TargetEntries");
        eref_colnames.lappend(mcx, Node::mk_string(mcx, te.resname.unwrap_or("?column?"))?)?;
        // C copyObject(te->expr): the sealed expr subtree is shared read-only.
        groupexprs.lappend(mcx, te.expr)?;
        nscolumns.push(ParseNamespaceColumn {
            p_varno: 0,
            p_varattno: i as AttrNumber + 1,
            p_vartype: node_funcs::expr_type(te.expr),
            p_vartypmod: node_funcs::expr_typmod(te.expr),
            p_varcollid: node_funcs::expr_collation(te.expr),
            p_varreturningtype: VarReturningType::VAR_RETURNING_DEFAULT,
            p_varnosyn: 0,
            p_varattnosyn: i as AttrNumber + 1,
            p_dontexpand: false,
        });
    }
    let eref =
        Node::mk_mut(mcx, Alias { aliasname: Some("*GROUP*"), colnames: eref_colnames })?
            .seal_ref();

    let rte = RangeTblEntry {
        rtekind: RTEKind::RTE_GROUP,
        alias: None,
        eref: Some(eref),
        groupexprs,
        lateral: false,
        inFromCl: false,
        ..Default::default()
    };
    let rte_node = Node::mk(mcx, rte)?;
    pstate.p_rtable.lappend(mcx, rte_node)?;
    let rtindex = pstate.p_rtable.len() as i32;
    for c in nscolumns.iter_mut() {
        c.p_varno = rtindex as Index;
        c.p_varnosyn = rtindex as Index;
    }

    let rte = rte_node.as_range_tbl_entry().expect("just built");
    let nsitem = ParseNamespaceItem {
        p_names: rte.eref.expect("rte has eref"),
        p_rte: rte_node,
        p_rtindex: rtindex,
        p_perminfo: None,
        p_nscolumns: nscolumns.leak(),
        p_rel_visible: true,
        p_cols_visible: true,
        p_lateral_only: core::cell::Cell::new(false),
        p_lateral_ok: core::cell::Cell::new(true),
        p_returning_type: VarReturningType::VAR_RETURNING_DEFAULT,
    };
    Ok(mcx::leak_in(mcx::alloc_in(mcx, nsitem)?))
}

// buildVarFromNSColumn (parse_relation.c): joinaliasvars entries; no column
// SELECT privilege is requested here.
pub fn buildVarFromNSColumn<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    nscol: &ParseNamespaceColumn,
) -> PgResult<Node<'mcx>> {
    debug_assert!(nscol.p_varno > 0);
    let mut var = Var {
        varno: nscol.p_varno as i32,
        varattno: nscol.p_varattno,
        vartype: nscol.p_vartype,
        vartypmod: nscol.p_vartypmod,
        varcollid: nscol.p_varcollid,
        varnullingrels: types_nodes::Bitmapset::empty(),
        varlevelsup: 0,
        varreturningtype: nscol.p_varreturningtype,
        varnosyn: nscol.p_varnosyn,
        varattnosyn: nscol.p_varattnosyn,
        location: -1,
    };
    markNullableIfNeeded(mcx, pstate, &mut var)?;
    Node::mk(mcx, var)
}

#[allow(clippy::too_many_arguments)]
pub fn addRangeTableEntryForJoin<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    colnames: &NodeList<'mcx>,
    nscolumns: PgVec<'mcx, ParseNamespaceColumn>,
    jointype: types_nodes::JoinType,
    nummergedcols: i32,
    aliasvars: NodeList<'mcx>,
    leftcols: types_nodes::list::IntList<'mcx>,
    rightcols: types_nodes::list::IntList<'mcx>,
    join_using_alias: Option<&'mcx Alias<'mcx>>,
    alias: Option<&'mcx Alias<'mcx>>,
    inFromCl: bool,
) -> PgResult<&'mcx mut ParseNamespaceItem<'mcx>> {
    if aliasvars.len() > i16::MAX as usize {
        return Err(too_many_join_columns());
    }

    let (aliasname, mut eref_colnames) = match alias {
        Some(a) => (a.aliasname, a.colnames.clone_in(mcx)?),
        None => (Some("unnamed_join"), NodeList::nil()),
    };
    let numaliases = eref_colnames.len();
    for i in numaliases..colnames.len() {
        eref_colnames.lappend(mcx, colnames.nth(i))?;
    }
    if numaliases > colnames.len() {
        return Err(too_many_join_aliases(aliasname.unwrap_or(""), colnames.len(), numaliases));
    }
    let eref = Node::mk_mut(mcx, Alias { aliasname, colnames: eref_colnames })?.seal_ref();

    // Joins are never checked for access rights: no addRTEPermissionInfo.
    let rte = RangeTblEntry {
        rtekind: RTEKind::RTE_JOIN,
        jointype,
        joinmergedcols: nummergedcols,
        joinaliasvars: aliasvars,
        joinleftcols: leftcols,
        joinrightcols: rightcols,
        join_using_alias,
        alias,
        eref: Some(eref),
        lateral: false,
        inFromCl,
        ..Default::default()
    };
    let rte_node = Node::mk(mcx, rte)?;
    pstate.p_rtable.lappend(mcx, rte_node)?;
    let rtindex = pstate.p_rtable.len() as i32;

    let rte = rte_node.as_range_tbl_entry().expect("just built");
    let nsitem = ParseNamespaceItem {
        p_names: rte.eref.expect("rte has eref"),
        p_rte: rte_node,
        p_rtindex: rtindex,
        p_perminfo: None,
        p_nscolumns: nscolumns.leak(),
        p_rel_visible: true,
        p_cols_visible: true,
        p_lateral_only: core::cell::Cell::new(false),
        p_lateral_ok: core::cell::Cell::new(true),
        p_returning_type: VarReturningType::VAR_RETURNING_DEFAULT,
    };
    Ok(mcx::leak_in(mcx::alloc_in(mcx, nsitem)?))
}

pub fn attnameAttNum(rel: &Relation<'_>, attname: &str, sys_col_ok: bool) -> AttrNumber {
    for i in 0..rel.rd_att.natts as usize {
        let att = rel.rd_att.attr(i);
        if att.attname.name_str() == attname.as_bytes() && !att.attisdropped {
            return (i + 1) as AttrNumber;
        }
    }
    if sys_col_ok {
        let i = specialAttNum(attname);
        if i != InvalidAttrNumber {
            return i;
        }
    }
    InvalidAttrNumber
}

pub fn addRTEPermissionInfo<'mcx>(
    mcx: Mcx<'mcx>,
    rteperminfos: &mut NodeList<'mcx>,
    rte: &mut RangeTblEntry<'mcx>,
) -> PgResult<Node<'mcx>> {
    debug_assert!(OidIsValid(rte.relid));
    debug_assert_eq!(rte.perminfoindex, 0);

    let perminfo = Node::mk(
        mcx,
        RTEPermissionInfo { relid: rte.relid, inh: rte.inh, ..Default::default() },
    )?;
    rteperminfos.lappend(mcx, perminfo)?;
    rte.perminfoindex = rteperminfos.len() as Index;
    Ok(perminfo)
}

pub fn getRTEPermissionInfo<'mcx>(
    rteperminfos: &NodeList<'mcx>,
    rte: &RangeTblEntry<'mcx>,
) -> PgResult<Node<'mcx>> {
    if rte.perminfoindex == 0 || rte.perminfoindex as usize > rteperminfos.len() {
        return Err(bad_perminfo_index(rte));
    }
    let node = rteperminfos.nth(rte.perminfoindex as usize - 1);
    let perminfo = node.as_rte_permission_info().expect("rteperminfos holds RTEPermissionInfo");
    if perminfo.relid != rte.relid {
        return Err(perminfo_relid_mismatch(rte, perminfo.relid));
    }
    Ok(node)
}

pub fn checkNameSpaceConflicts(
    namespace1: &[&ParseNamespaceItem<'_>],
    namespace2: &[&ParseNamespaceItem<'_>],
) -> PgResult<()> {
    for nsitem1 in namespace1 {
        let rte1 = nsitem1.rte();
        let aliasname1 = nsitem1.p_names.aliasname;
        if !nsitem1.p_rel_visible {
            continue;
        }
        for nsitem2 in namespace2 {
            let rte2 = nsitem2.rte();
            if !nsitem2.p_rel_visible {
                continue;
            }
            if nsitem2.p_names.aliasname != aliasname1 {
                continue;
            }
            if rte1.rtekind == RTEKind::RTE_RELATION
                && rte1.alias.is_none()
                && rte2.rtekind == RTEKind::RTE_RELATION
                && rte2.alias.is_none()
                && rte1.relid != rte2.relid
            {
                continue;
            }
            return Err(duplicate_table_name(aliasname1.unwrap_or("")));
        }
    }
    Ok(())
}

pub fn addNSItemToQuery<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    nsitem: &'mcx mut ParseNamespaceItem<'mcx>,
    addToJoinList: bool,
    addToRelNameSpace: bool,
    addToVarNameSpace: bool,
) -> PgResult<()> {
    if addToJoinList {
        let rtr = Node::mk_range_tbl_ref(mcx, nsitem.p_rtindex)?;
        pstate.p_joinlist.lappend(mcx, rtr)?;
    }
    if addToRelNameSpace || addToVarNameSpace {
        nsitem.p_rel_visible = addToRelNameSpace;
        nsitem.p_cols_visible = addToVarNameSpace;
        nsitem.p_lateral_only.set(false);
        nsitem.p_lateral_ok.set(true);
        pstate.p_namespace.push(nsitem);
    }
    Ok(())
}

pub fn expandRTE<'mcx>(
    mcx: Mcx<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    rtindex: i32,
    sublevels_up: i32,
    returning_type: VarReturningType,
    location: ParseLoc,
    include_dropped: bool,
) -> PgResult<(NodeList<'mcx>, NodeList<'mcx>)> {
    match rte.rtekind {
        RTEKind::RTE_RELATION => expandRelation(
            mcx,
            rte.relid,
            rte.eref.expect("relation RTE has eref"),
            rtindex,
            sublevels_up,
            returning_type,
            location,
            include_dropped,
        ),
        RTEKind::RTE_FUNCTION => expandFunction(
            mcx,
            rte,
            rtindex,
            sublevels_up,
            returning_type,
            location,
            include_dropped,
        ),
        RTEKind::RTE_SUBQUERY => {
            let eref = rte.eref.expect("subquery RTE has eref");
            let subquery = rte.subquery.expect("RTE_SUBQUERY has subquery");
            let mut colnames = NodeList::nil();
            let mut colvars = NodeList::nil();
            let mut varattno = 0usize;
            for te_node in &subquery.targetList {
                let te = te_node.as_target_entry().expect("tlist cell");
                if te.resjunk {
                    continue;
                }
                varattno += 1;
                debug_assert_eq!(varattno, te.resno as usize);
                if varattno > eref.colnames.len() {
                    return Err(Box::new(PgError::error(format!(
                        "too few column names for subquery {}",
                        eref.aliasname.unwrap_or("")
                    ))));
                }
                colnames.lappend(mcx, eref.colnames.nth(varattno - 1))?;
                colvars.lappend(
                    mcx,
                    Node::mk(
                        mcx,
                        Var {
                            varno: rtindex,
                            varattno: varattno as AttrNumber,
                            vartype: node_funcs::expr_type(te.expr),
                            vartypmod: node_funcs::expr_typmod(te.expr),
                            varcollid: node_funcs::expr_collation(te.expr),
                            varnullingrels: types_nodes::Bitmapset::empty(),
                            varlevelsup: sublevels_up as Index,
                            varreturningtype: returning_type,
                            varnosyn: rtindex as Index,
                            varattnosyn: varattno as AttrNumber,
                            location,
                        },
                    )?,
                )?;
            }
            Ok((colnames, colvars))
        }
        // C shares this leg across TABLEFUNC/VALUES/CTE/ENR (coltypes lists +
        // eref colnames); a dropped ENR column has coltype InvalidOid.
        RTEKind::RTE_TABLEFUNC
        | RTEKind::RTE_VALUES
        | RTEKind::RTE_CTE
        | RTEKind::RTE_NAMEDTUPLESTORE => {
            let mut colnames = NodeList::nil();
            let mut colvars = NodeList::nil();
            let eref = rte.eref.expect("coltypes-list RTE has eref");
            for varattno in 0..rte.coltypes.len() {
                let coltype = rte.coltypes.nth(varattno);
                if coltype == InvalidOid {
                    if include_dropped {
                        colnames.lappend(mcx, Node::mk_string(mcx, "")?)?;
                        // C makeNullConst: the claimed type doesn't matter.
                        colvars.lappend(
                            mcx,
                            Node::mk_const(
                                mcx,
                                types_core::catalog::INT4OID,
                                -1,
                                InvalidOid,
                                4,
                                ::datum::Datum::null(),
                                true,
                                true,
                            )?,
                        )?;
                    }
                    continue;
                }
                colnames.lappend(mcx, eref.colnames.nth(varattno))?;
                colvars.lappend(
                    mcx,
                    Node::mk(
                        mcx,
                        Var {
                            varno: rtindex,
                            varattno: varattno as AttrNumber + 1,
                            vartype: coltype,
                            vartypmod: rte.coltypmods.nth(varattno),
                            varcollid: rte.colcollations.nth(varattno),
                            varnullingrels: types_nodes::Bitmapset::empty(),
                            varlevelsup: sublevels_up as Index,
                            varreturningtype: returning_type,
                            varnosyn: rtindex as Index,
                            varattnosyn: varattno as AttrNumber + 1,
                            location,
                        },
                    )?,
                )?;
            }
            Ok((colnames, colvars))
        }
        RTEKind::RTE_JOIN => {
            expandJoin(mcx, rte, rtindex, sublevels_up, returning_type, location)
        }
        // These expose no columns.
        RTEKind::RTE_RESULT | RTEKind::RTE_GROUP => Ok((NodeList::nil(), NodeList::nil())),
    }
}

// expandRTE's RTE_JOIN arm: a simple-Var joinaliasvars entry is copied with
// adjusted varlevelsup; a merged USING column becomes a join alias Var,
// matching expandNSItemVars' output for "join.*".
fn expandJoin<'mcx>(
    mcx: Mcx<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    rtindex: i32,
    sublevels_up: i32,
    returning_type: VarReturningType,
    location: ParseLoc,
) -> PgResult<(NodeList<'mcx>, NodeList<'mcx>)> {
    let eref = rte.eref.expect("join RTE has eref");
    assert_eq!(eref.colnames.len(), rte.joinaliasvars.len());
    let mut colnames = NodeList::nil();
    let mut colvars = NodeList::nil();
    for (i, (colname, avar)) in eref.colnames.iter().zip(rte.joinaliasvars.iter()).enumerate() {
        colnames.lappend(mcx, colname)?;
        let varnode = if let Some(v) = avar.as_var() {
            Var {
                varno: v.varno,
                varattno: v.varattno,
                vartype: v.vartype,
                vartypmod: v.vartypmod,
                varcollid: v.varcollid,
                varnullingrels: v.varnullingrels.clone_in(mcx)?,
                varlevelsup: sublevels_up as Index,
                varreturningtype: returning_type,
                varnosyn: v.varnosyn,
                varattnosyn: v.varattnosyn,
                location,
            }
        } else {
            Var {
                varno: rtindex,
                varattno: i as AttrNumber + 1,
                vartype: node_funcs::expr_type(avar),
                vartypmod: node_funcs::expr_typmod(avar),
                varcollid: node_funcs::expr_collation(avar),
                varnullingrels: types_nodes::Bitmapset::empty(),
                varlevelsup: sublevels_up as Index,
                varreturningtype: returning_type,
                varnosyn: rtindex as Index,
                varattnosyn: i as AttrNumber + 1,
                location,
            }
        };
        colvars.lappend(mcx, Node::mk(mcx, varnode)?)?;
    }
    Ok((colnames, colvars))
}

fn expandFunction<'mcx>(
    mcx: Mcx<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    rtindex: i32,
    sublevels_up: i32,
    returning_type: VarReturningType,
    location: ParseLoc,
    include_dropped: bool,
) -> PgResult<(NodeList<'mcx>, NodeList<'mcx>)> {
    let eref = rte.eref.expect("function RTE has eref");
    let mut colnames = NodeList::nil();
    let mut colvars = NodeList::nil();
    let mut atts_done: usize = 0;

    let mk_var = |vartype: Oid, vartypmod: i32, varcollid: Oid, attno: i32| {
        Node::mk(
            mcx,
            Var {
                varno: rtindex,
                varattno: attno as AttrNumber,
                vartype,
                vartypmod,
                varcollid,
                varnullingrels: types_nodes::Bitmapset::empty(),
                varlevelsup: sublevels_up as Index,
                varreturningtype: returning_type,
                varnosyn: rtindex as Index,
                varattnosyn: attno as AttrNumber,
                location,
            },
        )
    };

    for f in &rte.functions {
        let rtfunc = f
            .as_range_tbl_function()
            .expect("functions cell is RangeTblFunction");
        let funcexpr = rtfunc.funcexpr.expect("RangeTblFunction has funcexpr");

        if !rtfunc.funccolnames.is_nil() {
            // Coldeflist leg (RECORD-returning).
            for (k, name) in eref
                .colnames
                .as_slice()
                .iter()
                .skip(atts_done)
                .take(rtfunc.funccolcount as usize)
                .enumerate()
            {
                colnames.lappend(mcx, *name)?;
                let attnum = atts_done + k + 1;
                colvars.lappend(
                    mcx,
                    mk_var(
                        rtfunc.funccoltypes.nth(k),
                        rtfunc.funccoltypmods.nth(k),
                        rtfunc.funccolcollations.nth(k),
                        attnum as i32,
                    )?,
                )?;
            }
        } else {
            let resolved = funcapi::get_expr_result_type(mcx, Some(funcexpr))?;
            match resolved.class {
                funcapi::TypeFuncClass::Composite
                | funcapi::TypeFuncClass::CompositeDomain => {
                    let tupdesc =
                        resolved.result_tuple_desc.expect("composite result carries a tupdesc");
                    debug_assert_eq!(rtfunc.funccolcount, tupdesc.natts);
                    let (names, vars) = expandTupleDesc(
                        mcx,
                        &tupdesc,
                        eref,
                        rtfunc.funccolcount as usize,
                        atts_done,
                        rtindex,
                        sublevels_up,
                        returning_type,
                        location,
                        include_dropped,
                    )?;
                    for n in &names {
                        colnames.lappend(mcx, n)?;
                    }
                    for v in &vars {
                        colvars.lappend(mcx, v)?;
                    }
                }
                funcapi::TypeFuncClass::Scalar => {
                    let typmod = nodes_core::node_funcs::expr_typmod(funcexpr);
                    let collation = nodes_core::node_funcs::expr_collation(funcexpr);
                    colnames.lappend(mcx, eref.colnames.nth(atts_done))?;
                    colvars.lappend(
                        mcx,
                        mk_var(
                            resolved.result_type_id,
                            typmod,
                            collation,
                            atts_done as i32 + 1,
                        )?,
                    )?;
                }
                other => panic!(
                    "expandRTE: function in FROM has unsupported return type ({other:?})"
                ),
            }
        }
        atts_done += rtfunc.funccolcount as usize;
    }

    if rte.funcordinality {
        colnames.lappend(mcx, *eref.colnames.as_slice().last().expect("ordinality alias"))?;
        colvars.lappend(
            mcx,
            mk_var(types_core::catalog::INT8OID, -1, InvalidOid, atts_done as i32 + 1)?,
        )?;
    }

    Ok((colnames, colvars))
}

#[allow(clippy::too_many_arguments)]
fn expandRelation<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    eref: &Alias<'mcx>,
    rtindex: i32,
    sublevels_up: i32,
    returning_type: VarReturningType,
    location: ParseLoc,
    include_dropped: bool,
) -> PgResult<(NodeList<'mcx>, NodeList<'mcx>)> {
    let rel = relation_seams::relation_open::call(mcx, relid, AccessShareLock)?;
    let natts = rel.rd_att.natts as usize;
    let result = expandTupleDesc(
        mcx,
        &rel.rd_att,
        eref,
        natts,
        0,
        rtindex,
        sublevels_up,
        returning_type,
        location,
        include_dropped,
    );
    rel.close(AccessShareLock)?;
    result
}

#[allow(clippy::too_many_arguments)]
fn expandTupleDesc<'mcx>(
    mcx: Mcx<'mcx>,
    tupdesc: &TupleDescData<'mcx>,
    eref: &Alias<'mcx>,
    count: usize,
    offset: usize,
    rtindex: i32,
    sublevels_up: i32,
    returning_type: VarReturningType,
    location: ParseLoc,
    include_dropped: bool,
) -> PgResult<(NodeList<'mcx>, NodeList<'mcx>)> {
    let mut colnames = NodeList::nil();
    let mut colvars = NodeList::nil();
    let aliases = eref.colnames.as_slice();
    let mut aliascell = offset;

    debug_assert!(count <= tupdesc.natts as usize);
    for varattno in 0..count {
        let attr = tupdesc.attr(varattno);
        if attr.attisdropped {
            if include_dropped {
                colnames.lappend(mcx, Node::mk_string(mcx, "")?)?;
                // C emits a NULL Const here; the claimed type is arbitrary.
                colvars.lappend(
                    mcx,
                    Node::mk_const(
                        mcx,
                        types_core::catalog::INT4OID,
                        -1,
                        InvalidOid,
                        4,
                        datum::Datum::null(),
                        true,
                        true,
                    )?,
                )?;
            }
            if aliascell < aliases.len() {
                aliascell += 1;
            }
            continue;
        }

        let label = if aliascell < aliases.len() {
            let l = aliases[aliascell].as_string().expect("eref colnames are String nodes").sval;
            aliascell += 1;
            l
        } else {
            str_in(
                mcx,
                core::str::from_utf8(attr.attname.name_str()).expect("catalog attnames are UTF-8"),
            )?
        };
        colnames.lappend(mcx, Node::mk_string(mcx, label)?)?;

        colvars.lappend(
            mcx,
            Node::mk(
                mcx,
                Var {
                    varno: rtindex,
                    varattno: (varattno + offset) as AttrNumber + 1,
                    vartype: attr.atttypid,
                    vartypmod: attr.atttypmod,
                    varcollid: attr.attcollation,
                    varnullingrels: types_nodes::Bitmapset::empty(),
                    varlevelsup: sublevels_up as Index,
                    varreturningtype: returning_type,
                    varnosyn: rtindex as Index,
                    varattnosyn: (varattno + offset) as AttrNumber + 1,
                    location,
                },
            )?,
        )?;
    }
    Ok((colnames, colvars))
}

pub fn expandNSItemVars<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    nsitem: &ParseNamespaceItem<'mcx>,
    sublevels_up: i32,
    location: ParseLoc,
) -> PgResult<(NodeList<'mcx>, NodeList<'mcx>)> {
    let mut vars = NodeList::nil();
    let mut colnames = NodeList::nil();
    for (colindex, colnameval) in nsitem.p_names.colnames.iter().enumerate() {
        let colname = colnameval.as_string().expect("eref colnames are String nodes").sval;
        let nscol = &nsitem.p_nscolumns[colindex];
        if nscol.p_dontexpand {
            continue;
        }
        if !colname.is_empty() {
            debug_assert!(nscol.p_varno > 0);
            let mut var = Var {
                varno: nscol.p_varno as i32,
                varattno: nscol.p_varattno,
                vartype: nscol.p_vartype,
                vartypmod: nscol.p_vartypmod,
                varcollid: nscol.p_varcollid,
                varnullingrels: types_nodes::Bitmapset::empty(),
                varlevelsup: sublevels_up as Index,
                varreturningtype: nscol.p_varreturningtype,
                varnosyn: nscol.p_varnosyn,
                varattnosyn: nscol.p_varattnosyn,
                location,
            };
            markNullableIfNeeded(mcx, pstate, &mut var)?;
            vars.lappend(mcx, Node::mk(mcx, var)?)?;
            colnames.lappend(mcx, colnameval)?;
        } else {
            debug_assert_eq!(nscol.p_varno, 0);
        }
    }
    Ok((vars, colnames))
}

pub fn expandNSItemAttrs<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    nsitem: &ParseNamespaceItem<'mcx>,
    sublevels_up: i32,
    require_col_privs: bool,
    location: ParseLoc,
) -> PgResult<NodeList<'mcx>> {
    let rte = nsitem.rte();
    let (vars, names) = expandNSItemVars(mcx, pstate, nsitem, sublevels_up, location)?;
    let mut te_list = NodeList::nil();

    if rte.rtekind == RTEKind::RTE_RELATION {
        let perminfo = nsitem.p_perminfo.expect("relation nsitem has perminfo");
        // SAFETY: perminfo nodes are read only through transient as_* lookups;
        // no derived reference is live across this call.
        unsafe { perminfo.with_mut::<RTEPermissionInfo, _>(|p| p.requiredPerms |= ACL_SELECT) }
            .expect("p_perminfo is RTEPermissionInfo");
    }

    debug_assert_eq!(names.len(), vars.len());
    for (name, var_node) in names.iter().zip(vars.iter()) {
        let label = name.as_string().expect("colnames are String nodes").sval;
        let resno = pstate.p_next_resno as AttrNumber;
        pstate.p_next_resno += 1;
        te_list.lappend(mcx, Node::mk_target_entry(mcx, var_node, resno, Some(label), false)?)?;
        if require_col_privs {
            let var = var_node.as_var().expect("expandNSItemVars yields Vars");
            markVarForSelectPriv(mcx, pstate, var)?;
        }
    }
    Ok(te_list)
}

pub fn errorMissingRTE<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    relation: &types_nodes::RangeVar<'_>,
) -> Box<PgError> {
    let rte = match searchRangeTableForRel(mcx, pstate, relation) {
        Ok(rte) => rte,
        Err(e) => return e,
    };
    let relname = relation.relname.expect("grammar always sets relname");

    let mut badAlias: Option<&str> = None;
    if let Some(rte) = rte {
        let eref_alias = rte.eref.and_then(|e| e.aliasname).unwrap_or("");
        if rte.alias.is_some() && eref_alias != relname {
            let mut sublevels_up = 0;
            match refnameNamespaceItem(
                pstate,
                None,
                eref_alias,
                relation.location,
                Some(&mut sublevels_up),
            ) {
                Ok(Some(nsitem)) if core::ptr::eq(nsitem.rte(), rte) => {
                    badAlias = Some(eref_alias);
                }
                Ok(_) => {}
                Err(e) => return e,
            }
        }
    }

    let b = elog::ereport(ERROR).errcode(ERRCODE_UNDEFINED_TABLE);
    let b = if let Some(badAlias) = badAlias {
        b.errmsg(format!("invalid reference to FROM-clause entry for table \"{relname}\""))
            .errhint(format!("Perhaps you meant to reference the table alias \"{badAlias}\"."))
    } else if let Some(rte) = rte {
        let eref_alias = rte.eref.and_then(|e| e.aliasname).unwrap_or("");
        let b = b
            .errmsg(format!("invalid reference to FROM-clause entry for table \"{relname}\""))
            .errdetail(format!(
                "There is an entry for table \"{eref_alias}\", but it cannot be referenced \
                 from this part of the query."
            ));
        if rte_visible_if_lateral(pstate, rte) {
            b.errhint("To reference that table, you must mark this subquery with LATERAL.")
        } else {
            b
        }
    } else {
        b.errmsg(format!("missing FROM-clause entry for table \"{relname}\""))
    };
    Box::new(
        b.errposition(errpos(pstate, relation.location))
            .into_error()
            .with_error_location(loc("errorMissingRTE")),
    )
}

fn searchRangeTableForRel<'p, 'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &'p ParseState<'p, 'mcx>,
    relation: &types_nodes::RangeVar<'_>,
) -> PgResult<Option<&'mcx RangeTblEntry<'mcx>>> {
    let refname = relation.relname.expect("grammar always sets relname");

    // An unqualified name may be a CTE (which hides real relations) or an ENR.
    let (cte_levelsup, isenr) = if relation.schemaname.is_none() {
        match scanNameSpaceForCTE(pstate, refname) {
            Some((_, levelsup)) => (Some(levelsup), false),
            None => (None, parser_small1::name_matches_visible_ENR(pstate, refname)),
        }
    } else {
        (None, false)
    };

    let relId = if cte_levelsup.is_none() && !isenr {
        namespace_seams::range_var_get_relid::call(mcx, &to_rel_vocab(relation), NoLock, true)?
    } else {
        InvalidOid
    };

    let mut ps = Some(pstate);
    let mut levelsup: Index = 0;
    while let Some(p) = ps {
        for rte_node in &p.p_rtable {
            let rte = rte_node.as_range_tbl_entry().expect("rtable holds RangeTblEntry");
            if rte.rtekind == RTEKind::RTE_RELATION && OidIsValid(relId) && rte.relid == relId {
                return Ok(Some(rte));
            }
            if rte.rtekind == RTEKind::RTE_CTE
                && cte_levelsup.is_some_and(|c| rte.ctelevelsup + levelsup == c)
                && rte.ctename == Some(refname)
            {
                return Ok(Some(rte));
            }
            if rte.rtekind == RTEKind::RTE_NAMEDTUPLESTORE
                && isenr
                && rte.enrname == Some(refname)
            {
                return Ok(Some(rte));
            }
            if rte.eref.and_then(|e| e.aliasname) == Some(refname) {
                return Ok(Some(rte));
            }
        }
        ps = p.parentParseState;
        levelsup += 1;
    }
    Ok(None)
}

pub fn errorMissingColumn(
    mcx: Mcx<'_>,
    pstate: &ParseState<'_, '_>,
    relname: Option<&str>,
    colname: &str,
    location: ParseLoc,
) -> Box<PgError> {
    let state = match searchRangeTableForCol(mcx, pstate, relname, colname, location) {
        Ok(state) => state,
        Err(e) => return e,
    };

    let msg = match relname {
        Some(relname) => format!("column {relname}.{colname} does not exist"),
        None => format!("column \"{colname}\" does not exist"),
    };

    if let Some(rexact1) = state.rexact1 {
        let b = elog::ereport(ERROR).errcode(ERRCODE_UNDEFINED_COLUMN).errmsg(msg);
        let b = if state.rexact2.is_some() {
            let b = b.errdetail(format!(
                "There are columns named \"{colname}\", but they are in tables that \
                 cannot be referenced from this part of the query."
            ));
            if relname.is_none() {
                b.errhint("Try using a table-qualified name.")
            } else {
                b
            }
        } else {
            let eref_alias = rexact1.eref.and_then(|e| e.aliasname).unwrap_or("");
            let b = b.errdetail(format!(
                "There is a column named \"{colname}\" in table \"{eref_alias}\", but it \
                 cannot be referenced from this part of the query."
            ));
            if rte_visible_if_lateral(pstate, rexact1) {
                b.errhint("To reference that column, you must mark this subquery with LATERAL.")
            } else if relname.is_none() && rte_visible_if_qualified(pstate, rexact1) {
                b.errhint("To reference that column, you must use a table-qualified name.")
            } else {
                b
            }
        };
        return Box::new(
            b.errposition(errpos(pstate, location))
                .into_error()
                .with_error_location(loc("errorMissingColumn")),
        );
    }

    fn colname_of<'a>(rte: &'a RangeTblEntry<'a>, attnum: AttrNumber) -> &'a str {
        rte.eref
            .expect("analyzed RTE always has eref")
            .colnames
            .nth(attnum as usize - 1)
            .as_string()
            .expect("eref colnames are String nodes")
            .sval
    }
    fn alias_of<'a>(rte: &'a RangeTblEntry<'a>) -> &'a str {
        rte.eref.and_then(|e| e.aliasname).unwrap_or("")
    }

    let b = elog::ereport(ERROR).errcode(ERRCODE_UNDEFINED_COLUMN).errmsg(msg);
    let b = match (state.rfirst, state.rsecond) {
        (Some(rfirst), None) => b.errhint(format!(
            "Perhaps you meant to reference the column \"{}.{}\".",
            alias_of(rfirst),
            colname_of(rfirst, state.first)
        )),
        (Some(rfirst), Some(rsecond)) => b.errhint(format!(
            "Perhaps you meant to reference the column \"{}.{}\" or the column \"{}.{}\".",
            alias_of(rfirst),
            colname_of(rfirst, state.first),
            alias_of(rsecond),
            colname_of(rsecond, state.second)
        )),
        _ => b,
    };
    Box::new(
        b.errposition(errpos(pstate, location))
            .into_error()
            .with_error_location(loc("errorMissingColumn")),
    )
}

fn findNSItemForRTE<'p, 'mcx>(
    pstate: &'p ParseState<'p, 'mcx>,
    rte: &RangeTblEntry<'mcx>,
) -> Option<&'mcx ParseNamespaceItem<'mcx>> {
    let mut ps = Some(pstate);
    while let Some(p) = ps {
        for nsitem in p.p_namespace.iter().copied() {
            if core::ptr::eq(nsitem.rte(), rte) {
                return Some(nsitem);
            }
        }
        ps = p.parentParseState;
    }
    None
}

fn rte_visible_if_lateral<'p, 'mcx>(
    pstate: &'p ParseState<'p, 'mcx>,
    rte: &RangeTblEntry<'mcx>,
) -> bool {
    if pstate.p_lateral_active {
        return false;
    }
    match findNSItemForRTE(pstate, rte) {
        Some(nsitem) => nsitem.p_lateral_only.get() && nsitem.p_lateral_ok.get(),
        None => false,
    }
}

fn rte_visible_if_qualified<'p, 'mcx>(
    pstate: &'p ParseState<'p, 'mcx>,
    rte: &RangeTblEntry<'mcx>,
) -> bool {
    match findNSItemForRTE(pstate, rte) {
        Some(nsitem) => nsitem.p_rel_visible && !nsitem.p_cols_visible,
        None => false,
    }
}

fn to_rel_vocab<'a>(rv: &types_nodes::RangeVar<'a>) -> rel_vocab::RangeVar<'a> {
    rel_vocab::RangeVar {
        catalogname: rv.catalogname,
        schemaname: rv.schemaname,
        relname: rv.relname.expect("grammar always sets relname"),
        inh: rv.inh,
        relpersistence: rv.relpersistence,
        location: rv.location,
    }
}

fn str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let bytes = mcx::slice_borrow_in(mcx, s.as_bytes())?;
    // SAFETY: byte-for-byte copy of a &str.
    Ok(unsafe { core::str::from_utf8_unchecked(bytes) })
}

#[track_caller]
#[cold]
#[inline(never)]
fn ambiguous_table_ref(
    pstate: &ParseState<'_, '_>,
    refname: &str,
    location: ParseLoc,
) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_AMBIGUOUS_ALIAS)
            .errmsg(format!("table reference \"{refname}\" is ambiguous"))
            .errposition(errpos(pstate, location))
            .into_error()
            .with_error_location(loc("scanNameSpaceForRefname")),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn ambiguous_table_relid(
    pstate: &ParseState<'_, '_>,
    relid: Oid,
    location: ParseLoc,
) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_AMBIGUOUS_ALIAS)
            .errmsg(format!("table reference {relid} is ambiguous"))
            .errposition(errpos(pstate, location))
            .into_error()
            .with_error_location(loc("scanNameSpaceForRelid")),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn ambiguous_column_ref(
    pstate: &ParseState<'_, '_>,
    colname: &str,
    location: ParseLoc,
) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_AMBIGUOUS_COLUMN)
            .errmsg(format!("column reference \"{colname}\" is ambiguous"))
            .errposition(errpos(pstate, location))
            .into_error()
            .with_error_location(loc("colNameToVar")),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn bad_lateral_ref<'p, 'mcx>(
    pstate: &'p ParseState<'p, 'mcx>,
    nsitem: &ParseNamespaceItem<'mcx>,
    location: ParseLoc,
) -> Box<PgError> {
    let refname = nsitem.p_names.aliasname.unwrap_or("");
    let is_target =
        pstate.p_target_nsitem.is_some_and(|t| t.p_rte.ptr_eq(nsitem.p_rte));
    let b = elog::ereport(ERROR)
        .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
        .errmsg(format!("invalid reference to FROM-clause entry for table \"{refname}\""));
    let b = if is_target {
        b.errhint(format!(
            "There is an entry for table \"{refname}\", but it cannot be referenced from \
             this part of the query."
        ))
    } else {
        b.errdetail("The combining JOIN type must be INNER or LEFT for a LATERAL reference.")
    };
    Box::new(
        b.errposition(errpos(pstate, location))
            .into_error()
            .with_error_location(loc("check_lateral_ref_ok")),
    )
}

enum SysColContext {
    CheckConstraint,
    GeneratedColumn,
    MergeWhen,
}

#[track_caller]
#[cold]
#[inline(never)]
fn bad_system_column_context(
    pstate: &ParseState<'_, '_>,
    colname: &str,
    location: ParseLoc,
    what: SysColContext,
) -> Box<PgError> {
    let msg = match what {
        SysColContext::CheckConstraint => {
            format!("system column \"{colname}\" reference in check constraint is invalid")
        }
        SysColContext::GeneratedColumn => {
            format!("cannot use system column \"{colname}\" in column generation expression")
        }
        SysColContext::MergeWhen => {
            format!("cannot use system column \"{colname}\" in MERGE WHEN condition")
        }
    };
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            .errmsg(msg)
            .errposition(errpos(pstate, location))
            .into_error()
            .with_error_location(loc("scanNSItemForColumn")),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn dropped_column(nsitem: &ParseNamespaceItem<'_>, colname: &str) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_COLUMN)
            .errmsg(format!(
                "column \"{}\" of relation \"{}\" does not exist",
                colname,
                nsitem.p_names.aliasname.unwrap_or("")
            ))
            .into_error()
            .with_error_location(loc("scanNSItemForColumn")),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn undefined_table(
    pstate: &ParseState<'_, '_>,
    relation: &types_nodes::RangeVar<'_>,
) -> Box<PgError> {
    let relname = relation.relname.expect("grammar always sets relname");
    let msg = match relation.schemaname {
        Some(schemaname) => format!("relation \"{schemaname}.{relname}\" does not exist"),
        None => format!("relation \"{relname}\" does not exist"),
    };
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_TABLE)
            .errmsg(msg)
            .errposition(errpos(pstate, relation.location))
            .into_error()
            .with_error_location(loc("parserOpenTable")),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn unsupported_function_return_type(
    pstate: &ParseState<'_, '_>,
    funcname: &str,
    rettype: Oid,
    location: ParseLoc,
) -> Box<PgError> {
    let typename = format_type::format_type_be(rettype)
        .unwrap_or_else(|_| format!("type {rettype}"));
    Box::new(
        elog::ereport(ERROR)
            .errcode(types_error::ERRCODE_DATATYPE_MISMATCH)
            .errmsg(format!(
                "function \"{funcname}\" in FROM has unsupported return type {typename}"
            ))
            .errposition(errpos(pstate, location))
            .into_error()
            .with_error_location(loc("addRangeTableEntryForFunction")),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn too_many_aliases(aliasname: &str, available: usize, specified: usize) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            .errmsg(format!(
                "table \"{aliasname}\" has {available} columns available but {specified} \
                 columns specified"
            ))
            .into_error()
            .with_error_location(loc("buildRelationAliases")),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn too_many_join_columns() -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg(format!("joins can have at most {} columns", i16::MAX))
            .into_error()
            .with_error_location(loc("addRangeTableEntryForJoin")),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn too_many_join_aliases(aliasname: &str, available: usize, specified: usize) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            .errmsg(format!(
                "join expression \"{aliasname}\" has {available} columns available but \
                 {specified} columns specified"
            ))
            .into_error()
            .with_error_location(loc("addRangeTableEntryForJoin")),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn duplicate_table_name(aliasname: &str) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_ALIAS)
            .errmsg(format!("table name \"{aliasname}\" specified more than once"))
            .into_error()
            .with_error_location(loc("checkNameSpaceConflicts")),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn bad_perminfo_index(rte: &RangeTblEntry<'_>) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "invalid perminfoindex {} in RTE with relid {}",
            rte.perminfoindex, rte.relid
        ))
        .with_error_location(loc("getRTEPermissionInfo")),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn perminfo_relid_mismatch(rte: &RangeTblEntry<'_>, perminfo_relid: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "permission info at index {} (with relid={}) does not match provided RTE \
             (with relid={})",
            rte.perminfoindex, perminfo_relid, rte.relid
        ))
        .with_error_location(loc("getRTEPermissionInfo")),
    )
}

struct UsesTempRelation<'mcx> {
    mcx: Mcx<'mcx>,
}

impl<'mcx> UsesTempRelation<'mcx> {
    fn query(&mut self, q: &types_nodes::parsenodes::Query<'mcx>) -> PgResult<bool> {
        for item in q.rtable.iter() {
            let rte = item.as_range_tbl_entry().expect("rtable entry");
            if rte.rtekind == RTEKind::RTE_RELATION {
                let rel = relation_seams::relation_open::call(self.mcx, rte.relid, AccessShareLock)?;
                let relpersistence = rel.rd_rel.relpersistence;
                rel.close(AccessShareLock)?;
                if relpersistence == types_core::catalog::RELPERSISTENCE_TEMP {
                    return Ok(true);
                }
            }
        }
        nodes_core::query_tree_walker(q, self, nodes_core::QTW_IGNORE_JOINALIASES)
    }
}

impl<'mcx> nodes_core::NodeWalker<'mcx> for UsesTempRelation<'mcx> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if let Some(q) = node.as_query() {
            return self.query(q);
        }
        nodes_core::expression_tree_walker(node, self)
    }

    fn visit_query_ref(&mut self, q: &'mcx types_nodes::parsenodes::Query<'mcx>) -> PgResult<bool> {
        self.query(q)
    }
}

pub fn isQueryUsingTempRelation<'mcx>(
    mcx: Mcx<'mcx>,
    query: &types_nodes::parsenodes::Query<'mcx>,
) -> PgResult<bool> {
    UsesTempRelation { mcx }.query(query)
}
