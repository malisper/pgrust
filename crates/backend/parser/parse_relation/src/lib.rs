#![allow(non_snake_case)]

#[cfg(test)]
mod tests;

use mcx::{Mcx, PgVec};
use parser_small1::{
    parser_errposition, ParseNamespaceColumn, ParseNamespaceItem, ParseState,
};
use types_core::{AttrNumber, Index, InvalidOid, Oid, OidIsValid, ParseLoc};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_AMBIGUOUS_ALIAS, ERRCODE_AMBIGUOUS_COLUMN,
    ERRCODE_DUPLICATE_ALIAS, ERRCODE_INVALID_COLUMN_REFERENCE, ERRCODE_UNDEFINED_COLUMN,
    ERRCODE_UNDEFINED_TABLE, ERROR,
};
use types_nodes::parsenodes::ACL_SELECT;
use types_nodes::{
    Alias, Node, NodeList, RTEKind, RTEPermissionInfo, RangeTblEntry, Var, VarReturningType,
};
use types_rel::{AccessShareLock, NoLock, Relation, RowShareLock, LOCKMODE};
use types_tuple::htup::FirstLowInvalidHeapAttributeNumber;
use types_tuple::tupdesc::TupleDescData;

const InvalidAttrNumber: AttrNumber = 0;

fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("parse_relation.c", 0, funcname)
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
        if nsitem.p_lateral_only && !pstate.p_lateral_active {
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
        let rte = nsitem.p_rte;
        if !nsitem.p_rel_visible {
            continue;
        }
        if nsitem.p_lateral_only && !pstate.p_lateral_active {
            continue;
        }
        if nsitem.p_returning_type != VarReturningType::VAR_RETURNING_DEFAULT {
            continue;
        }
        if rte.rtekind == RTEKind::RTE_RELATION && rte.relid == relid && rte.alias.is_none() {
            if result.is_some() {
                return Err(ambiguous_table_ref(pstate, &relid.to_string(), location));
            }
            check_lateral_ref_ok(pstate, nsitem, location)?;
            result = Some(nsitem);
        }
    }
    Ok(result)
}

fn check_lateral_ref_ok(
    pstate: &ParseState<'_, '_>,
    nsitem: &ParseNamespaceItem<'_>,
    location: ParseLoc,
) -> PgResult<()> {
    if nsitem.p_lateral_only && !nsitem.p_lateral_ok {
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
    let rte = nsitem.p_rte;
    let attnum = scanRTEForColumn(pstate, rte, nsitem.p_names, colname, location)?;

    if attnum == InvalidAttrNumber {
        return Ok(None);
    }
    // C's CHECK_CONSTRAINT/GENERATED_COLUMN/MERGE_WHEN system-column ereports
    // guard attnum < 0 only; unreachable while specialAttNum panics on match.
    debug_assert!(attnum > InvalidAttrNumber);

    let nscol = &nsitem.p_nscolumns[attnum as usize - 1];
    if nscol.p_varno == 0 {
        return Err(dropped_column(nsitem, colname));
    }
    let mut var = Var {
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
    };
    markNullableIfNeeded(mcx, pstate, &mut var)?;
    markVarForSelectPriv(mcx, pstate, &var)?;
    Node::mk(mcx, var).map(Some)
}

fn scanRTEForColumn(
    pstate: &ParseState<'_, '_>,
    rte: &RangeTblEntry<'_>,
    eref: &Alias<'_>,
    colname: &str,
    location: ParseLoc,
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
    }

    if result != InvalidAttrNumber {
        return Ok(result);
    }

    if rte.rtekind == RTEKind::RTE_RELATION && rte.relkind != types_rel::RELKIND_COMPOSITE_TYPE {
        specialAttNum(colname);
    }

    Ok(InvalidAttrNumber)
}

// C SystemAttributeByName over heap.c's SysAtt rows; names verified vs
// sysattr.h. A match panics: the negative-attnum Var lane (typed via
// SystemAttributeDefinition + ATTNUM syscache probe) is unported.
fn specialAttNum(attname: &str) -> AttrNumber {
    if matches!(attname, "ctid" | "xmin" | "cmin" | "xmax" | "cmax" | "tableoid") {
        panic!(
            "specialAttNum (parse_relation.c): system column \"{attname}\" needs \
             SystemAttributeDefinition (heap.c) — unit backend-parser-relation \
             system-column lane"
        );
    }
    InvalidAttrNumber
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
            if nsitem.p_lateral_only && !p.p_lateral_active {
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

struct ExactAttrMatchState<'mcx> {
    rexact1: Option<&'mcx RangeTblEntry<'mcx>>,
    rexact2: Option<&'mcx RangeTblEntry<'mcx>>,
}

// C's searchRangeTableForCol also tracks Levenshtein near-matches; only the
// exact-match half is live here (see errorMissingColumn).
fn searchRangeTableForCol<'p, 'mcx>(
    pstate: &'p ParseState<'p, 'mcx>,
    colname: &str,
    location: ParseLoc,
) -> PgResult<ExactAttrMatchState<'mcx>> {
    let orig_pstate = pstate;
    let mut state = ExactAttrMatchState { rexact1: None, rexact2: None };

    let mut ps = Some(pstate);
    while let Some(p) = ps {
        for rte_node in &p.p_rtable {
            let rte = rte_node.as_range_tbl_entry().expect("rtable holds RangeTblEntry");
            if rte.rtekind == RTEKind::RTE_JOIN {
                continue;
            }
            let eref = rte.eref.expect("analyzed RTE always has eref");
            let attnum = scanRTEForColumn(orig_pstate, rte, eref, colname, location)?;
            if attnum != InvalidAttrNumber {
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
        RTEKind::RTE_JOIN => panic!(
            "markRTEForSelectPriv (parse_relation.c): RTE_JOIN arm (whole-row/USING \
             propagation) unported — unit backend-parser-relation join lane"
        ),
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
    rte: &'mcx RangeTblEntry<'mcx>,
    rtindex: i32,
    perminfo: Option<Node<'mcx>>,
    tupdesc: &TupleDescData<'mcx>,
) -> PgResult<ParseNamespaceItem<'mcx>> {
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
        p_rte: rte,
        p_rtindex: rtindex,
        p_perminfo: perminfo,
        p_nscolumns: nscolumns.leak(),
        p_rel_visible: true,
        p_cols_visible: true,
        p_lateral_only: false,
        p_lateral_ok: true,
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
            if relation.schemaname.is_none() && !pstate.p_future_ctes.is_nil() {
                panic!(
                    "parserOpenTable (parse_relation.c): isFutureCTE hint needs the \
                     CommonTableExpr vocabulary — unit backend-parser-medium1"
                );
            }
            Err(undefined_table(pstate, relation))
        }
    }
}

pub fn isLockedRefname(pstate: &ParseState<'_, '_>, _refname: Option<&str>) -> bool {
    if pstate.p_locked_from_parent {
        return true;
    }
    if !pstate.p_locking_clause.is_nil() {
        panic!(
            "isLockedRefname (parse_relation.c): LockingClause scan unported \
             (FOR UPDATE/SHARE lane) — unit backend-parser-relation"
        );
    }
    false
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
        rte_node.as_range_tbl_entry().expect("just built"),
        rtindex,
        Some(perminfo),
        &rel.rd_att,
    )?;

    table::table_close(rel, NoLock)?;

    Ok(mcx::leak_in(mcx::alloc_in(mcx, nsitem)?))
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
        let rte1 = nsitem1.p_rte;
        let aliasname1 = nsitem1.p_names.aliasname;
        if !nsitem1.p_rel_visible {
            continue;
        }
        for nsitem2 in namespace2 {
            let rte2 = nsitem2.p_rte;
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
        nsitem.p_lateral_only = false;
        nsitem.p_lateral_ok = true;
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
        other => panic!(
            "expandRTE (parse_relation.c): arm for {other:?} unported — \
             unit backend-parser-relation"
        ),
    }
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
    let rte = nsitem.p_rte;
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
                Ok(Some(nsitem)) if core::ptr::eq(nsitem.p_rte, rte) => {
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

    if relation.schemaname.is_none()
        && (!pstate.p_ctenamespace.is_nil() || !pstate.p_future_ctes.is_nil())
    {
        panic!(
            "searchRangeTableForRel (parse_relation.c): CTE/ENR scan needs the \
             CommonTableExpr vocabulary — unit backend-parser-medium1"
        );
    }

    let relId =
        namespace_seams::range_var_get_relid::call(mcx, &to_rel_vocab(relation), NoLock, true)?;

    let mut ps = Some(pstate);
    while let Some(p) = ps {
        for rte_node in &p.p_rtable {
            let rte = rte_node.as_range_tbl_entry().expect("rtable holds RangeTblEntry");
            if rte.rtekind == RTEKind::RTE_RELATION && OidIsValid(relId) && rte.relid == relId {
                return Ok(Some(rte));
            }
            if rte.eref.and_then(|e| e.aliasname) == Some(refname) {
                return Ok(Some(rte));
            }
        }
        ps = p.parentParseState;
    }
    Ok(None)
}

pub fn errorMissingColumn(
    pstate: &ParseState<'_, '_>,
    relname: Option<&str>,
    colname: &str,
    location: ParseLoc,
) -> Box<PgError> {
    let state = match searchRangeTableForCol(pstate, colname, location) {
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

    // C decides between the bald 42703 and a "Perhaps you meant" hint via
    // Levenshtein over every candidate column.
    panic!(
        "errorMissingColumn (parse_relation.c): fuzzy-hint lane needs \
         varstr_levenshtein_less_equal (levenshtein.c) — exact-match arms are live; \
         no exact match anywhere for column \"{colname}\""
    );
}

fn findNSItemForRTE<'p, 'mcx>(
    pstate: &'p ParseState<'p, 'mcx>,
    rte: &RangeTblEntry<'mcx>,
) -> Option<&'mcx ParseNamespaceItem<'mcx>> {
    let mut ps = Some(pstate);
    while let Some(p) = ps {
        for nsitem in p.p_namespace.iter().copied() {
            if core::ptr::eq(nsitem.p_rte, rte) {
                return Some(nsitem);
            }
        }
        ps = p.parentParseState;
    }
    None
}

fn rte_visible_if_lateral(pstate: &ParseState<'_, '_>, rte: &RangeTblEntry<'_>) -> bool {
    if pstate.p_lateral_active {
        return false;
    }
    match findNSItemForRTE(pstate, rte) {
        Some(nsitem) => nsitem.p_lateral_only && nsitem.p_lateral_ok,
        None => false,
    }
}

fn rte_visible_if_qualified(pstate: &ParseState<'_, '_>, rte: &RangeTblEntry<'_>) -> bool {
    match findNSItemForRTE(pstate, rte) {
        Some(nsitem) => nsitem.p_rel_visible && !nsitem.p_cols_visible,
        None => false,
    }
}

fn to_rel_vocab<'a>(rv: &'a types_nodes::RangeVar<'a>) -> rel_vocab::RangeVar<'a> {
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

#[cold]
#[inline(never)]
fn bad_lateral_ref(
    pstate: &ParseState<'_, '_>,
    nsitem: &ParseNamespaceItem<'_>,
    location: ParseLoc,
) -> Box<PgError> {
    let refname = nsitem.p_names.aliasname.unwrap_or("");
    let is_target =
        pstate.p_target_nsitem.is_some_and(|t| core::ptr::eq(t.p_rte, nsitem.p_rte));
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
