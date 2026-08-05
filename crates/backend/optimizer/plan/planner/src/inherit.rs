//! inherit.c + appendinfo.c slice: expand inheritance/partition parents into
//! appendrel children, including inherited-target DML row identity
//! (ROWID_VAR/RowIdentityVarInfo). Inherited RowMarks are loud.

use mcx::{alloc_leak_in, Mcx, PgVec};
use types_error::PgResult;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{RTEKind, RangeTblEntry};
use types_nodes::primnodes::{Alias, Var, ROWID_VAR};
use types_nodes::{Node, NodeList, NodeTag};
use types_pathnodes::{AppendRelInfo, NodeId, RelId, RowIdentityVarInfo, RELOPT_BASEREL};

use crate::run::PlannerRun;

// add_other_rels_to_query (initsplan.c).
pub fn add_other_rels_to_query(run: &mut PlannerRun<'_>) -> PgResult<()> {
    for rti in 1..run.root.simple_rel_array_size as usize {
        let Some(rel) = run.root.simple_rel_array[rti] else { continue };
        if run.root.rel(rel).reloptkind != RELOPT_BASEREL {
            continue;
        }
        let rte = run.rte(rti);
        if !rte.inh {
            continue;
        }
        match rte.rtekind {
            RTEKind::RTE_RELATION => expand_inherited_rtentry(run, rel, rti)?,
            RTEKind::RTE_SUBQUERY => expand_appendrel_subquery(run, rel, rti)?,
            other => panic!("add_other_rels_to_query (initsplan.c): inh {other:?}"),
        }
    }
    Ok(())
}

// expand_appendrel_subquery (inherit.c): UNION ALL children already have RTEs
// and AppendRelInfos from prepjointree; just build their RelOptInfos.
fn expand_appendrel_subquery(run: &mut PlannerRun<'_>, rel: RelId, rti: usize) -> PgResult<()> {
    for ai in 0..run.root.append_rel_list.len() {
        let child_rti = {
            let a = &run.root.append_rel_list[ai];
            if a.parent_relid != rti as u32 {
                continue;
            }
            a.child_relid
        };
        debug_assert!((child_rti as i32) < run.root.simple_rel_array_size);
        let childrel = crate::relnode::build_simple_rel_child(run, child_rti, rel)?;
        let childrte = run.rte(child_rti as usize);
        if childrte.inh {
            match childrte.rtekind {
                RTEKind::RTE_RELATION => {
                    expand_inherited_rtentry(run, childrel, child_rti as usize)?
                }
                RTEKind::RTE_SUBQUERY => {
                    expand_appendrel_subquery(run, childrel, child_rti as usize)?
                }
                other => panic!("expand_appendrel_subquery (inherit.c): inh {other:?}"),
            }
        }
    }
    Ok(())
}

// expand_inherited_rtentry (inherit.c).
fn expand_inherited_rtentry<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    rti: usize,
) -> PgResult<()> {
    let mcx = run.mcx;
    let rte = run.rte(rti);
    debug_assert!(rte.inh && rte.rtekind == RTEKind::RTE_RELATION);
    let parent_oid = rte.relid;
    let lockmode = rte.rellockmode;

    // get_plan_rowmark: a parent selected FOR UPDATE/SHARE keeps its mark as
    // isParent and every child gets its own (inherit.c:124-136).
    let mut oldrc: Option<types_pathnodes::PlanRowMarkId> = None;
    let mut old_is_parent = false;
    let mut old_all_mark_types = 0i32;
    for &rm in run.root.rowMarks.iter() {
        if run.rowmark(rm).rti == rti as u32 {
            oldrc = Some(rm);
            let rc = run.rowmark_mut(rm);
            old_is_parent = rc.isParent;
            rc.isParent = true;
            old_all_mark_types = rc.allMarkTypes;
            break;
        }
    }
    let perminfoindex = rte.perminfoindex;

    let oldrelation = table::table_open(mcx, parent_oid, types_rel::NoLock)?;
    if oldrelation.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE {
        let updated_cols = {
            let parse = run.parse();
            debug_assert!(perminfoindex > 0);
            parse
                .rteperminfos
                .nth(perminfoindex as usize - 1)
                .as_rte_permission_info()
                .expect("rteperminfos cell")
                .updatedCols
                .clone_in(mcx)?
        };
        expand_partitioned_rtentry(run, rel, rti, &oldrelation, &updated_cols, oldrc, lockmode)?;
    } else {
        let inh_oids = pg_inherits::find_all_inheritors(mcx, parent_oid, lockmode)?;
        debug_assert!(inh_oids.first() == Some(&parent_oid));
        for &child_oid in inh_oids.iter() {
            let newrelation = if child_oid != parent_oid {
                let r = table::table_open(mcx, child_oid, types_rel::NoLock)?;
                // Other sessions' temp children cannot be accessed; C silently
                // omits them from the appendrel (inherit.c).
                if r.rd_rel.relpersistence == types_core::RELPERSISTENCE_TEMP
                    && !r.rd_islocaltemp
                {
                    r.close(lockmode)?;
                    continue;
                }
                Some(r)
            } else {
                None
            };
            {
                let childrel = newrelation.as_ref().unwrap_or(&oldrelation);
                let child_rti =
                    expand_single_inheritance_child(run, rti, &oldrelation, childrel, oldrc)?;
                crate::relnode::build_simple_rel_child(run, child_rti, rel)?;
            }
            if let Some(r) = newrelation {
                r.close(types_rel::NoLock)?;
            }
        }
    }

    // Children may have reported new mark types into the parent mark; add the
    // junk columns preprocess_targetlist would have added for them, plus the
    // dispatch tableoid (inherit.c:229-308).
    if let Some(rm) = oldrc {
        use types_nodes::plannodes::RowMarkType;
        let rc = *run.rowmark(rm);
        let new_all_mark_types = rc.allMarkTypes;
        let copy_bit = 1 << RowMarkType::ROW_MARK_COPY as i32;
        let mut newvars: Vec<Node<'mcx>> = Vec::new();
        if new_all_mark_types & !copy_bit != 0 && old_all_mark_types & !copy_bit == 0 {
            let var = mk_var(
                mcx,
                rc.rti,
                types_tuple::htup::SelfItemPointerAttributeNumber as i16,
                types_core::catalog::TIDOID,
                -1,
                0,
            )?;
            let resname = crate::prep::arena_str(mcx, &format!("ctid{}", rc.rowmarkId))?;
            let resno = run.processed_tlist.expect("processed_tlist set").len() as i16 + 1;
            let tle = Node::mk_target_entry(mcx, var, resno, Some(resname), true)?;
            processed_tlist_append(run, tle)?;
            newvars.push(var);
        }
        // A child reported ROW_MARK_COPY where the parent had none (foreign
        // children); add the wholerow junk var (makeWholeRowVar on the parent
        // RTE). Unreachable until FDW children land (select_rowmark_type's
        // foreign arm is loud), ported for C parity.
        if new_all_mark_types & copy_bit != 0 && old_all_mark_types & copy_bit == 0 {
            let rte = run.rte(rc.rti as usize);
            let var = Node::mk(
                mcx,
                nodes_core::makefuncs::make_whole_row_var(mcx, rte, rc.rti, 0, false)?,
            )?;
            let resname = crate::prep::arena_str(mcx, &format!("wholerow{}", rc.rowmarkId))?;
            let resno = run.processed_tlist.expect("processed_tlist set").len() as i16 + 1;
            let tle = Node::mk_target_entry(mcx, var, resno, Some(resname), true)?;
            processed_tlist_append(run, tle)?;
            newvars.push(var);
        }
        if !old_is_parent {
            let var = mk_var(
                mcx,
                rc.rti,
                types_tuple::htup::TableOidAttributeNumber as i16,
                types_core::catalog::OIDOID,
                -1,
                0,
            )?;
            let resname = crate::prep::arena_str(mcx, &format!("tableoid{}", rc.rowmarkId))?;
            let resno = run.processed_tlist.expect("processed_tlist set").len() as i16 + 1;
            let tle = Node::mk_target_entry(mcx, var, resno, Some(resname), true)?;
            processed_tlist_append(run, tle)?;
            newvars.push(var);
        }
        if !newvars.is_empty() {
            let relids0 = crate::relnode::relids_singleton(mcx, 0);
            crate::initsplan::add_vars_to_targetlist(run, &newvars, &relids0)?;
        }
    }
    oldrelation.close(types_rel::NoLock)
}

// expand_partitioned_rtentry (inherit.c): plan-time pruning picks the live
// partitions; only those are locked and expanded.
fn expand_partitioned_rtentry<'mcx>(
    run: &mut PlannerRun<'mcx>,
    relinfo: RelId,
    parent_rti: usize,
    parentrel: &types_rel::Relation<'mcx>,
    parent_updated_cols: &types_nodes::Bitmapset<'mcx>,
    // The TOP parent's rowmark (not the intermediate partitioned child's):
    // every descendant mark shares its rowmarkId and junk columns.
    top_parentrc: Option<types_pathnodes::PlanRowMarkId>,
    lockmode: i32,
) -> PgResult<()> {
    let mcx = run.mcx;
    debug_assert!(run.rte(parent_rti).inh);
    if !run.root.partColsUpdated {
        run.root.partColsUpdated = has_partition_attrs(mcx, parentrel, parent_updated_cols)?;
    }
    let pdesc = partdesc::RelationGetPartitionDesc(parentrel, true)?;
    let live_parts = crate::partprune::prune_append_rel_partitions(run, relinfo)?;
    let oids = {
        let mut v: PgVec<'mcx, types_core::Oid> = mcx::vec_with_capacity_in(mcx, pdesc.oids.len())?;
        v.extend(pdesc.oids.iter().copied());
        v
    };
    {
        let r = run.root.rel_mut(relinfo);
        r.part_rels = mcx::vec_from_elem_in(mcx, None, oids.len());
        r.all_partrels = crate::relnode::relids_empty();
    }
    {
        let mut lp: types_pathnodes::Relids<'mcx> = crate::relnode::relids_empty();
        let mut m = live_parts.next_member(-1);
        while m >= 0 {
            lp = crate::relnode::relids_add_member(mcx, &lp, m as u32);
            m = live_parts.next_member(m);
        }
        run.root.rel_mut(relinfo).live_parts = lp;
    }
    let mut i = live_parts.next_member(-1);
    while i >= 0 {
        let child_oid = oids[i as usize];
        let childrel = table::table_open(mcx, child_oid, lockmode)?;
        assert!(
            !(childrel.rd_rel.relpersistence == types_core::RELPERSISTENCE_TEMP
                && !childrel.rd_islocaltemp),
            "temporary relation from another session found as partition"
        );
        let child_rti =
            expand_single_inheritance_child(run, parent_rti, parentrel, &childrel, top_parentrc)?;
        let childrelinfo = crate::relnode::build_simple_rel_child(run, child_rti, relinfo)?;
        run.root.rel_mut(relinfo).part_rels[i as usize] = Some(childrelinfo);
        {
            let child_relids = crate::relnode::relids_copy(mcx, &run.root.rel(childrelinfo).relids);
            let cur = crate::relnode::relids_take(&mut run.root.rel_mut(relinfo).all_partrels);
            run.root.rel_mut(relinfo).all_partrels =
                crate::relnode::relids_union(mcx, &cur, &child_relids);
        }
        if childrel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE {
            let appinfo = run.root.append_rel_array[child_rti as usize]
                .clone()
                .expect("child AppendRelInfo");
            let child_updated_cols = translate_col_privs(run, parent_updated_cols, &appinfo)?;
            expand_partitioned_rtentry(
                run,
                childrelinfo,
                child_rti as usize,
                &childrel,
                &child_updated_cols,
                top_parentrc,
                lockmode,
            )?;
        }
        childrel.close(types_rel::NoLock)?;
        i = live_parts.next_member(i);
    }
    Ok(())
}

// expand_single_inheritance_child (inherit.c): child RTE + AppendRelInfo.
fn expand_single_inheritance_child<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parent_rti: usize,
    parentrel: &types_rel::Relation<'mcx>,
    childrel: &types_rel::Relation<'mcx>,
    top_parentrc: Option<types_pathnodes::PlanRowMarkId>,
) -> PgResult<u32> {
    let mcx = run.mcx;
    let parentrte = run.rte(parent_rti);
    debug_assert!(parentrte.rtekind == RTEKind::RTE_RELATION);
    let child_oid = childrel.rd_id;
    let child_relkind = childrel.rd_rel.relkind;

    // The RTI the child will get: current rtable length + 1.
    let child_rti = run.parse().rtable.len() as u32 + 1;
    let mut appinfo = make_append_rel_info(run, parentrel, childrel, parent_rti as u32, child_rti)?;

    let parent_eref = parentrte.eref.expect("RTE eref");
    let child_tupdesc = childrel.descr();
    let mut child_colnames = NodeList::nil();
    for cattno in 0..child_tupdesc.natts as usize {
        let att = child_tupdesc.attr(cattno);
        let pcolno = appinfo.parent_colnos[cattno];
        let attname: &'mcx str = if att.attisdropped {
            ""
        } else if pcolno > 0 && (pcolno as usize) <= parent_eref.colnames.len() {
            parent_eref
                .colnames
                .nth(pcolno as usize - 1)
                .as_string()
                .expect("eref colname")
                .sval
        } else {
            str_in(mcx, att.attname.name_str())?
        };
        child_colnames.lappend(mcx, Node::mk_string(mcx, attname)?)?;
    }
    let alias: &'mcx Alias<'mcx> = alloc_leak_in(
        mcx,
        Alias { aliasname: parent_eref.aliasname, colnames: child_colnames },
    )?;

    let childrte = RangeTblEntry {
        alias: Some(alias),
        eref: Some(alias),
        rtekind: RTEKind::RTE_RELATION,
        relid: child_oid,
        inh: child_relkind == types_rel::RELKIND_PARTITIONED_TABLE,
        relkind: child_relkind,
        rellockmode: parentrte.rellockmode,
        perminfoindex: 0,
        // C's flat-copy carries the parent's tablesample into each child.
        tablesample: parentrte.tablesample,
        subquery: None,
        security_barrier: parentrte.security_barrier,
        jointype: parentrte.jointype,
        joinmergedcols: parentrte.joinmergedcols,
        joinaliasvars: NodeList::nil(),
        joinleftcols: types_nodes::list::IntList::nil(),
        joinrightcols: types_nodes::list::IntList::nil(),
        join_using_alias: None,
        functions: NodeList::nil(),
        funcordinality: parentrte.funcordinality,
        tablefunc: None,
        values_lists: NodeList::nil(),
        ctename: parentrte.ctename,
        ctelevelsup: parentrte.ctelevelsup,
        self_reference: parentrte.self_reference,
        coltypes: types_nodes::list::OidList::nil(),
        coltypmods: types_nodes::list::IntList::nil(),
        colcollations: types_nodes::list::OidList::nil(),
        enrname: parentrte.enrname,
        enrtuples: parentrte.enrtuples,
        groupexprs: NodeList::nil(),
        lateral: parentrte.lateral,
        inFromCl: parentrte.inFromCl,
        securityQuals: NodeList::nil(),
    };
    let assigned_rti = add_child_rte(run, Node::mk(mcx, childrte)?)?;
    debug_assert_eq!(assigned_rti, child_rti);

    appinfo.child_relid = child_rti;
    run.root.append_rel_array[child_rti as usize] = Some(appinfo.clone());
    run.root.append_rel_list.push(appinfo);

    // Build the child's PlanRowMark if the parent is marked FOR UPDATE/SHARE
    // (inherit.c:119-147). Partitioned children get isParent marks — the
    // executor ignores them, but their existence locks the child.
    if let Some(prc_id) = top_parentrc {
        use types_nodes::plannodes::PlanRowMark;
        let prc = *run.rowmark(prc_id);
        let mark_type = {
            let child_rte = run.rte(child_rti as usize);
            crate::prep::select_rowmark_type(child_rte, prc.strength)
        };
        let child_mark = PlanRowMark {
            rti: child_rti,
            prti: prc.rti,
            rowmarkId: prc.rowmarkId,
            markType: mark_type,
            allMarkTypes: 1 << mark_type as i32,
            strength: prc.strength,
            waitPolicy: prc.waitPolicy,
            isParent: child_relkind == types_rel::RELKIND_PARTITIONED_TABLE,
        };
        run.rowmark_mut(prc_id).allMarkTypes |= 1 << mark_type as i32;
        let id = run.add_rowmark(child_mark);
        run.root.rowMarks.push(id);
    }

    if crate::relnode::relids_is_member(parent_rti as i32, &run.root.all_result_relids) {
        let cur = crate::relnode::relids_take(&mut run.root.all_result_relids);
        run.root.all_result_relids = crate::relnode::relids_add_member(mcx, &cur, child_rti);
        // Non-leaf partitions need no row identity info.
        if child_relkind != types_rel::RELKIND_PARTITIONED_TABLE {
            let cur = crate::relnode::relids_take(&mut run.root.leaf_result_relids);
            run.root.leaf_result_relids =
                crate::relnode::relids_add_member(mcx, &cur, child_rti);
            let rrvar = mk_var(
                mcx,
                child_rti,
                types_tuple::htup::TableOidAttributeNumber as i16,
                types_core::catalog::OIDOID,
                -1,
                0,
            )?;
            add_row_identity_var(run, rrvar, child_rti, "tableoid")?;
            add_row_identity_columns(run, child_rti, child_relkind)?;
        }
    }
    Ok(child_rti)
}

// make_append_rel_info + make_inh_translation_list (appendinfo.c).
fn make_append_rel_info<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parentrel: &types_rel::Relation<'mcx>,
    childrel: &types_rel::Relation<'mcx>,
    parent_rti: u32,
    child_rti: u32,
) -> PgResult<AppendRelInfo<'mcx>> {
    let mcx = run.mcx;
    let mut appinfo = AppendRelInfo::new(mcx);
    appinfo.parent_relid = parent_rti;
    appinfo.child_relid = child_rti;
    appinfo.parent_reltype = parentrel.rd_rel.reltype;
    appinfo.child_reltype = childrel.rd_rel.reltype;
    appinfo.parent_reloid = parentrel.rd_id;

    let old_tupdesc = parentrel.descr();
    let new_tupdesc = childrel.descr();
    let oldnatts = old_tupdesc.natts as usize;
    let newnatts = new_tupdesc.natts as usize;
    appinfo.num_child_cols = newnatts as i32;
    appinfo.parent_colnos = mcx::vec_from_elem_in(mcx, 0i16, newnatts);
    appinfo.translated_vars = mcx::vec_with_capacity_in(mcx, oldnatts)?;

    let same_rel = parentrel.rd_id == childrel.rd_id;
    let mut new_attno = 0usize;
    for old_attno in 0..oldnatts {
        let att = old_tupdesc.attr(old_attno);
        if att.attisdropped {
            appinfo.translated_vars.push(NodeId::default());
            continue;
        }
        let (atttypid, atttypmod, attcollation) = (att.atttypid, att.atttypmod, att.attcollation);
        if same_rel {
            let var = mk_var(mcx, child_rti, (old_attno + 1) as i16, atttypid, atttypmod, attcollation)?;
            appinfo.translated_vars.push(run.intern_expr(var));
            appinfo.parent_colnos[old_attno] = (old_attno + 1) as i16;
            continue;
        }
        let attname = att.attname.name_str();
        let matches = |i: usize| {
            let a = new_tupdesc.attr(i);
            !a.attisdropped && a.attname.name_str() == attname
        };
        if new_attno >= newnatts || !matches(new_attno) {
            new_attno = (0..newnatts).find(|&i| matches(i)).unwrap_or_else(|| {
                panic!(
                    "could not find inherited attribute \"{}\" of relation \"{}\"",
                    String::from_utf8_lossy(attname),
                    childrel.name()
                )
            });
        }
        let catt = new_tupdesc.attr(new_attno);
        if atttypid != catt.atttypid || atttypmod != catt.atttypmod {
            return Err(attribute_mismatch(attname, childrel.name(), "type"));
        }
        if attcollation != catt.attcollation {
            return Err(attribute_mismatch(attname, childrel.name(), "collation"));
        }
        let var = mk_var(mcx, child_rti, (new_attno + 1) as i16, atttypid, atttypmod, attcollation)?;
        appinfo.translated_vars.push(run.intern_expr(var));
        appinfo.parent_colnos[new_attno] = (old_attno + 1) as i16;
        new_attno += 1;
    }
    Ok(appinfo)
}

fn mk_var<'mcx>(
    mcx: Mcx<'mcx>,
    varno: u32,
    varattno: i16,
    vartype: types_core::Oid,
    vartypmod: i32,
    varcollid: types_core::Oid,
) -> PgResult<Node<'mcx>> {
    Node::mk(
        mcx,
        Var {
            varno: varno as i32,
            varattno,
            vartype,
            vartypmod,
            varcollid,
            varnosyn: varno,
            varattnosyn: varattno,
            location: -1,
            ..Var::default()
        },
    )
}

// adjust_appendrel_attrs (appendinfo.c), single-appinfo expression form. The
// output never shares mutable nodes with the input: Vars are rebuilt, Consts
// and Params copied, interior nodes copied by the mutator's copy-on-write.
pub fn adjust_appendrel_attrs<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: Node<'mcx>,
    appinfo: &AppendRelInfo<'mcx>,
) -> PgResult<Node<'mcx>> {
    adjust_appendrel_attrs_multi(run, node, core::slice::from_ref(appinfo))
}

struct AppinfoMap<'mcx> {
    parent_relid: u32,
    child_relid: u32,
    parent_reltype: types_core::Oid,
    child_reltype: types_core::Oid,
    // None = dropped parent column.
    translated: PgVec<'mcx, Option<Node<'mcx>>>,
    // Parent RTE eref colnames, snapshot only for typeless children (the
    // whole-row RowExpr arm is the sole reader).
    parent_colnames: Option<NodeList<'mcx>>,
}

// ROWID_VAR resolution state, snapshotted so the mutator needn't re-enter run.
struct RowidSnap<'mcx> {
    leaf_result_relids: types_pathnodes::Relids<'mcx>,
    // (rowidvar Var, rowidrels) per RowIdentityVarInfo, 1-based by varattno.
    rowids: PgVec<'mcx, (Node<'mcx>, types_pathnodes::Relids<'mcx>)>,
}

fn copy_var<'mcx>(mcx: Mcx<'mcx>, v: &Var<'mcx>) -> PgResult<Var<'mcx>> {
    Ok(Var {
        varno: v.varno,
        varattno: v.varattno,
        vartype: v.vartype,
        vartypmod: v.vartypmod,
        varcollid: v.varcollid,
        varnullingrels: v.varnullingrels.clone_in(mcx)?,
        varlevelsup: v.varlevelsup,
        varreturningtype: v.varreturningtype,
        varnosyn: v.varnosyn,
        varattnosyn: v.varattnosyn,
        location: v.location,
    })
}

pub(crate) fn make_null_const<'mcx>(
    mcx: Mcx<'mcx>,
    typ: types_core::Oid,
    typmod: i32,
    collid: types_core::Oid,
) -> PgResult<Node<'mcx>> {
    let (typlen, typbyval) = lsyscache::get_typlenbyval(typ)?;
    Node::mk(
        mcx,
        types_nodes::primnodes::Const {
            consttype: typ,
            consttypmod: typmod,
            constcollid: collid,
            constlen: typlen as i32,
            constvalue: datum::Datum::null(),
            constisnull: true,
            constbyval: typbyval,
            location: -1,
        },
    )
}

pub fn adjust_appendrel_attrs_multi<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: Node<'mcx>,
    appinfos: &[AppendRelInfo<'mcx>],
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    debug_assert!(!appinfos.is_empty());
    // The mutator can't re-enter run (appinfos may borrow it), so snapshot
    // the translated exprs up front.
    let mut maps: PgVec<'mcx, AppinfoMap<'mcx>> = PgVec::new_in(mcx);
    for appinfo in appinfos {
        let mut translated: PgVec<'mcx, Option<Node<'mcx>>> = PgVec::new_in(mcx);
        for &tid in appinfo.translated_vars.iter() {
            if tid == NodeId::default() {
                translated.push(None);
            } else {
                translated.push(Some(*run.root.expr_node(tid)));
            }
        }
        let parent_colnames = if !types_core::OidIsValid(appinfo.child_reltype) {
            Some(run.rte(appinfo.parent_relid as usize).eref.expect("RTE eref").colnames.clone_in(mcx)?)
        } else {
            None
        };
        maps.push(AppinfoMap {
            parent_relid: appinfo.parent_relid,
            child_relid: appinfo.child_relid,
            parent_reltype: appinfo.parent_reltype,
            child_reltype: appinfo.child_reltype,
            translated,
            parent_colnames,
        });
    }
    let snap = RowidSnap {
        leaf_result_relids: crate::relnode::relids_copy(mcx, &run.root.leaf_result_relids),
        rowids: {
            let mut v: PgVec<'mcx, (Node<'mcx>, types_pathnodes::Relids<'mcx>)> =
                PgVec::new_in(mcx);
            for ri in run.root.row_identity_vars.iter() {
                v.push((
                    *run.root.expr_node(ri.rowidvar),
                    crate::relnode::relids_copy(mcx, &ri.rowidrels),
                ));
            }
            v
        },
    };
    fn mutate<'mcx>(
        mcx: Mcx<'mcx>,
        node: Node<'mcx>,
        maps: &[AppinfoMap<'mcx>],
        snap: &RowidSnap<'mcx>,
    ) -> PgResult<Option<Node<'mcx>>> {
        match node.node_tag() {
            NodeTag::T_Var => {
                let v = node.as_var().expect("Var");
                let map = if v.varlevelsup == 0 {
                    maps.iter().find(|m| v.varno == m.parent_relid as i32)
                } else {
                    None
                };
                let Some(map) = map else {
                    if v.varlevelsup == 0 && v.varno == ROWID_VAR {
                        // At a leaf target rel the placeholder resolves to a
                        // concrete Var (or NULL if this leaf can't produce
                        // it); at non-leaf levels it passes through.
                        let mut leaf_relid: u32 = 0;
                        for m in maps {
                            if crate::relnode::relids_is_member(
                                m.child_relid as i32,
                                &snap.leaf_result_relids,
                            ) {
                                assert!(
                                    leaf_relid == 0,
                                    "cannot translate to multiple leaf relids"
                                );
                                leaf_relid = m.child_relid;
                            }
                        }
                        if leaf_relid != 0 {
                            let (rv, rowidrels) = &snap.rowids[v.varattno as usize - 1];
                            if crate::relnode::relids_is_member(leaf_relid as i32, rowidrels)
                            {
                                let mut newvar =
                                    copy_var(mcx, rv.as_var().expect("rowidvar"))?;
                                newvar.varno = leaf_relid as i32;
                                debug_assert!(newvar.varnullingrels.is_empty());
                                newvar.varnosyn = 0;
                                newvar.varattnosyn = 0;
                                return Ok(Some(Node::mk(mcx, newvar)?));
                            }
                            return Ok(Some(make_null_const(
                                mcx,
                                v.vartype,
                                v.vartypmod,
                                v.varcollid,
                            )?));
                        }
                    }
                    return Ok(Some(Node::mk(mcx, copy_var(mcx, v)?)?));
                };
                if v.varattno > 0 {
                    let t = map
                        .translated
                        .get(v.varattno as usize - 1)
                        .copied()
                        .flatten()
                        .unwrap_or_else(|| {
                            panic!("attribute {} of relation does not exist", v.varattno)
                        });
                    // C copyObject's the translation per substitution site so
                    // setrefs' in-place fixups never see a shared subtree.
                    if let Some(tv) = t.as_var() {
                        let mut newvar = copy_var(mcx, tv)?;
                        newvar.varreturningtype = v.varreturningtype;
                        // C merges (not copies): the child Var may carry
                        // nullingrel bits of its own.
                        if !v.varnullingrels.is_empty() {
                            let mut merged = newvar.varnullingrels.clone_in(mcx)?;
                            merged.add_members(mcx, &v.varnullingrels)?;
                            newvar.varnullingrels = merged;
                        }
                        Ok(Some(Node::mk(mcx, newvar)?))
                    } else {
                        assert!(
                            v.varnullingrels.is_empty(),
                            "adjust_appendrel_attrs (appendinfo.c): nulled parent Var \
                             over a non-Var UNION ALL translation"
                        );
                        debug_assert!(
                            v.varreturningtype
                                == types_nodes::primnodes::VarReturningType::VAR_RETURNING_DEFAULT
                        );
                        Ok(Some(crate::prepjointree::copy_expr(mcx, t, 0)?))
                    }
                } else if v.varattno == 0 {
                    if !types_core::OidIsValid(map.child_reltype) {
                        assert!(
                            v.varreturningtype
                                == types_nodes::primnodes::VarReturningType::VAR_RETURNING_DEFAULT,
                            "failed to apply returningtype to a non-Var"
                        );
                        assert!(
                            v.varnullingrels.is_empty(),
                            "failed to apply nullingrels to a non-Var"
                        );
                        let mut fields = NodeList::nil();
                        for t in map.translated.iter() {
                            let t = t.expect("UNION ALL translated_vars carry no dropped slots");
                            fields.lappend(mcx, crate::prepjointree::copy_expr(mcx, t, 0)?)?;
                        }
                        return Ok(Some(Node::mk(
                            mcx,
                            types_nodes::primnodes::RowExpr {
                                args: fields,
                                row_typeid: v.vartype,
                                row_format: types_nodes::CoercionForm::COERCE_IMPLICIT_CAST,
                                colnames: map
                                    .parent_colnames
                                    .as_ref()
                                    .expect("typeless child snapshot")
                                    .clone_in(mcx)?,
                                location: -1,
                            },
                        )?));
                    }
                    debug_assert_eq!(v.vartype, map.parent_reltype);
                    let mut newvar = copy_var(mcx, v)?;
                    newvar.varno = map.child_relid as i32;
                    newvar.varnosyn = 0;
                    newvar.varattnosyn = 0;
                    if map.parent_reltype != map.child_reltype {
                        newvar.vartype = map.child_reltype;
                        let arg = Node::mk(mcx, newvar)?;
                        return Ok(Some(Node::mk(
                            mcx,
                            types_nodes::ConvertRowtypeExpr {
                                arg,
                                resulttype: map.parent_reltype,
                                convertformat:
                                    types_nodes::CoercionForm::COERCE_IMPLICIT_CAST,
                                location: -1,
                            },
                        )?));
                    }
                    Ok(Some(Node::mk(mcx, newvar)?))
                } else {
                    let mut newvar = copy_var(mcx, v)?;
                    newvar.varno = map.child_relid as i32;
                    newvar.varnosyn = 0;
                    newvar.varattnosyn = 0;
                    Ok(Some(Node::mk(mcx, newvar)?))
                }
            }
            NodeTag::T_Const => Ok(Some(Node::mk(mcx, *node.as_const().expect("Const"))?)),
            NodeTag::T_Param => Ok(Some(Node::mk(mcx, *node.as_param().expect("Param"))?)),
            NodeTag::T_PlaceHolderVar => {
                let phv = node.as_place_holder_var().expect("PlaceHolderVar");
                let new_expr = mutate(mcx, phv.phexpr, maps, snap)?.unwrap_or(phv.phexpr);
                let phrels = if phv.phlevelsup == 0 {
                    // adjust_child_relids over phrels; phnullingrels needn't
                    // change (C appendinfo.c).
                    let mut out = types_nodes::Bitmapset::empty();
                    for m in phv.phrels.iter() {
                        match maps.iter().find(|mp| mp.parent_relid as i32 == m) {
                            Some(mp) => out.add_member(mcx, mp.child_relid as i32)?,
                            None => out.add_member(mcx, m)?,
                        }
                    }
                    out
                } else {
                    phv.phrels.clone_in(mcx)?
                };
                Ok(Some(Node::mk(
                    mcx,
                    types_nodes::primnodes::PlaceHolderVar {
                        phexpr: new_expr,
                        phrels,
                        phnullingrels: phv.phnullingrels.clone_in(mcx)?,
                        phid: phv.phid,
                        phlevelsup: phv.phlevelsup,
                    },
                )?))
            }
            NodeTag::T_CurrentOfExpr => {
                let c = node.as_current_of_expr().expect("CurrentOfExpr");
                let cvarno = maps
                    .iter()
                    .find(|m| m.parent_relid == c.cvarno)
                    .map_or(c.cvarno, |m| m.child_relid);
                Ok(Some(Node::mk(
                    mcx,
                    types_nodes::primnodes::CurrentOfExpr {
                        cvarno,
                        cursor_name: c.cursor_name,
                        cursor_param: c.cursor_param,
                    },
                )?))
            }
            _ => nodes_core::expression_tree_mutator(mcx, node, &mut |n| {
                mutate(mcx, n, maps, snap)
            }),
        }
    }
    Ok(mutate(mcx, node, &maps, &snap)?.unwrap_or(node))
}

// find_appinfos_by_relids (appendinfo.c); clones since schemes hand these to
// mutators that can't hold root borrows.
pub fn find_appinfos_by_relids<'mcx>(
    run: &PlannerRun<'mcx>,
    relids: &types_pathnodes::Relids<'mcx>,
) -> PgVec<'mcx, AppendRelInfo<'mcx>> {
    let mut out: PgVec<'mcx, AppendRelInfo<'mcx>> = PgVec::new_in(run.mcx);
    for i in crate::relnode::relids_members(relids) {
        match run.root.append_rel_array.get(i as usize).and_then(|a| a.clone()) {
            Some(appinfo) => out.push(appinfo),
            None => {
                // Outer-join relids carry no appinfo; a baserel missing one
                // is a bug.
                let is_baserel = run
                    .root
                    .simple_rel_array
                    .get(i as usize)
                    .is_some_and(|r| r.is_some());
                assert!(!is_baserel, "child rel {i} not found in append_rel_array");
            }
        }
    }
    out
}

// adjust_child_relids (appendinfo.c).
pub fn adjust_child_relids<'mcx>(
    mcx: Mcx<'mcx>,
    relids: &types_pathnodes::Relids<'mcx>,
    appinfos: &[AppendRelInfo<'mcx>],
) -> types_pathnodes::Relids<'mcx> {
    let mut result = crate::relnode::relids_copy(mcx, relids);
    for appinfo in appinfos {
        if crate::relnode::relids_is_member(appinfo.parent_relid as i32, &result) {
            result = crate::relnode::relids_del_member(mcx, &result, appinfo.parent_relid as i32);
            result = crate::relnode::relids_add_member(mcx, &result, appinfo.child_relid);
        }
    }
    result
}

// adjust_appendrel_attrs_multilevel (appendinfo.c).
#[allow(dead_code)]
pub fn adjust_appendrel_attrs_multilevel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    mut node: Node<'mcx>,
    childrel: RelId,
    parentrel: RelId,
) -> PgResult<Node<'mcx>> {
    let parent = run.root.rel(childrel).parent;
    if parent != Some(parentrel) {
        let up = parent.expect("childrel is not a child of parentrel");
        node = adjust_appendrel_attrs_multilevel(run, node, up, parentrel)?;
    }
    let relids = crate::relnode::relids_copy(run.mcx, &run.root.rel(childrel).relids);
    let appinfos = find_appinfos_by_relids(run, &relids);
    adjust_appendrel_attrs_multi(run, node, &appinfos)
}

// adjust_child_relids_multilevel (appendinfo.c).
#[allow(dead_code)]
pub fn adjust_child_relids_multilevel<'mcx>(
    run: &PlannerRun<'mcx>,
    relids: &types_pathnodes::Relids<'mcx>,
    childrel: RelId,
    parentrel: RelId,
) -> types_pathnodes::Relids<'mcx> {
    if !crate::relnode::relids_overlap(relids, &run.root.rel(parentrel).relids) {
        return crate::relnode::relids_copy(run.mcx, relids);
    }
    let mut relids = crate::relnode::relids_copy(run.mcx, relids);
    let parent = run.root.rel(childrel).parent;
    if parent != Some(parentrel) {
        let up = parent.expect("childrel is not a child of parentrel");
        relids = adjust_child_relids_multilevel(run, &relids, up, parentrel);
    }
    let appinfos = find_appinfos_by_relids(run, &run.root.rel(childrel).relids);
    adjust_child_relids(run.mcx, &relids, &appinfos)
}

// adjust_inherited_attnums (appendinfo.c).
pub fn adjust_inherited_attnums<'mcx>(
    run: &PlannerRun<'mcx>,
    attnums: &[i16],
    appinfo: &AppendRelInfo<'mcx>,
) -> PgVec<'mcx, i16> {
    debug_assert!(types_core::OidIsValid(appinfo.parent_reloid));
    let mut result: PgVec<'mcx, i16> = PgVec::new_in(run.mcx);
    for &parentattno in attnums {
        assert!(parentattno > 0, "attribute {parentattno} of relation does not exist");
        let childvar = appinfo
            .translated_vars
            .get(parentattno as usize - 1)
            .filter(|&&tid| tid != NodeId::default())
            .map(|&tid| *run.root.expr_node(tid))
            .and_then(|n| n.as_var())
            .unwrap_or_else(|| {
                panic!("attribute {parentattno} of relation does not exist")
            });
        result.push(childvar.varattno);
    }
    result
}

// adjust_inherited_attnums_multilevel (appendinfo.c).
pub fn adjust_inherited_attnums_multilevel<'mcx>(
    run: &PlannerRun<'mcx>,
    attnums: &[i16],
    child_relid: u32,
    top_parent_relid: u32,
) -> PgVec<'mcx, i16> {
    let appinfo = run.root.append_rel_array[child_relid as usize]
        .as_ref()
        .unwrap_or_else(|| panic!("child rel {child_relid} not found in append_rel_array"));
    if appinfo.parent_relid != top_parent_relid {
        let up = adjust_inherited_attnums_multilevel(
            run,
            attnums,
            appinfo.parent_relid,
            top_parent_relid,
        );
        return adjust_inherited_attnums(run, &up, appinfo);
    }
    adjust_inherited_attnums(run, attnums, appinfo)
}

// The RestrictInfo arm of adjust_appendrel_attrs_mutator (appendinfo.c):
// flat-copy, translate the clause, adjust the relid sets, reset the cached
// derivative fields.
pub fn adjust_child_rinfo<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rid: types_pathnodes::RinfoId,
    appinfos: &[AppendRelInfo<'mcx>],
) -> PgResult<types_pathnodes::RinfoId> {
    let mcx = run.mcx;
    let mut ri = run.root.rinfo(rid).clone();
    debug_assert!(ri.orclause.is_none());
    let clause = *run.root.expr_node(ri.clause);
    let clause = adjust_appendrel_attrs_multi(run, clause, appinfos)?;
    ri.clause = run.intern_expr(clause);
    ri.clause_relids = adjust_child_relids(mcx, &ri.clause_relids, appinfos);
    ri.required_relids = adjust_child_relids(mcx, &ri.required_relids, appinfos);
    ri.outer_relids = adjust_child_relids(mcx, &ri.outer_relids, appinfos);
    ri.left_relids = adjust_child_relids(mcx, &ri.left_relids, appinfos);
    ri.right_relids = adjust_child_relids(mcx, &ri.right_relids, appinfos);
    ri.eval_cost.startup = -1.0;
    ri.norm_selec = -1.0;
    ri.outer_selec = -1.0;
    // left_ec/right_ec stay: each child variable is implicitly equivalent to
    // its parent, so the clause is still a member of the same parent ECs.
    ri.left_em = None;
    ri.right_em = None;
    ri.scansel_cache = PgVec::new_in(mcx);
    ri.left_bucketsize = -1.0;
    ri.right_bucketsize = -1.0;
    ri.left_mcvfreq = -1.0;
    ri.right_mcvfreq = -1.0;
    Ok(run.root.alloc_rinfo(ri))
}

// apply_child_basequals (inherit.c).
pub fn apply_child_basequals<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parent_rel: RelId,
    child_rel: RelId,
    appinfo: &AppendRelInfo<'mcx>,
) -> PgResult<bool> {
    let mcx = run.mcx;
    let mut childquals: PgVec<'mcx, types_pathnodes::RinfoId> = PgVec::new_in(mcx);
    let mut cq_min_security = u32::MAX;
    let parent_rinfos =
        crate::relnode::pgvec_clone_shallow(mcx, &run.root.rel(parent_rel).baserestrictinfo);
    for &rid in parent_rinfos.iter() {
        let (clause, is_pushed_down, has_clone, is_clone, security_level) = {
            let ri = run.root.rinfo(rid);
            (
                *run.root.expr_node(ri.clause),
                ri.is_pushed_down,
                ri.has_clone,
                ri.is_clone,
                ri.security_level,
            )
        };
        let childqual = adjust_appendrel_attrs(run, clause, appinfo)?;
        let childqual = clauses::eval_const_expressions(mcx, childqual)?;
        if let Some(c) = childqual.as_const() {
            if c.constisnull || !c.constvalue.as_bool() {
                return Ok(false);
            }
            continue;
        }
        for onecq in &clauses::make_ands_implicit(mcx, Some(childqual))? {
            let pseudoconstant = !vars::contain_vars_of_level(onecq, 0)?
                && !clauses::contain_volatile_functions(onecq)?;
            if pseudoconstant {
                run.root.hasPseudoConstantQuals = true;
            }
            let childrinfo = crate::initsplan::make_restrictinfo(
                run,
                onecq,
                is_pushed_down,
                has_clone,
                is_clone,
                pseudoconstant,
                security_level,
                crate::relnode::relids_empty(),
                crate::relnode::relids_empty(),
                crate::relnode::relids_empty(),
            )?;
            if crate::initsplan::restriction_is_always_false(run, childrinfo) {
                return Ok(false);
            }
            if crate::initsplan::restriction_is_always_true(run, childrinfo) {
                continue;
            }
            childquals.push(childrinfo);
            cq_min_security = cq_min_security.min(security_level);
        }
    }
    // Child securityQuals (UNION-ALL appendrels only; inheritance children
    // never carry their own, see expand_single_inheritance_child). Like
    // process_security_barrier_quals but with no general deductions.
    let child_sq = &run.rte(appinfo.child_relid as usize).securityQuals;
    let mut security_level: u32 = 0;
    for qualset_node in child_sq {
        let qualset = qualset_node.as_list().expect("securityQuals cell is a List");
        for qual in qualset.iter() {
            let childrinfo = crate::initsplan::make_restrictinfo(
                run,
                qual,
                true,
                false,
                false,
                false,
                security_level,
                crate::relnode::relids_empty(),
                crate::relnode::relids_empty(),
                crate::relnode::relids_empty(),
            )?;
            childquals.push(childrinfo);
            cq_min_security = cq_min_security.min(security_level);
        }
        security_level += 1;
    }
    debug_assert!(security_level <= run.root.qual_security_level);
    let crel = run.root.rel_mut(child_rel);
    crel.baserestrictinfo = childquals;
    crel.baserestrict_min_security = cq_min_security;
    Ok(true)
}

fn str_in<'mcx>(mcx: Mcx<'mcx>, raw: &[u8]) -> PgResult<&'mcx str> {
    let bytes = mcx::slice_borrow_in(mcx, raw)?;
    // SAFETY: attnames are valid UTF-8 byte-for-byte copies.
    Ok(core::str::from_utf8(bytes).expect("attname UTF-8"))
}

#[cold]
#[inline(never)]
fn attribute_mismatch(
    attname: &[u8],
    relname: &str,
    what: &str,
) -> Box<types_error::PgError> {
    Box::new(
        types_error::PgError::new(
            types_error::ERROR,
            format!(
                "attribute \"{}\" of relation \"{relname}\" does not match parent's {what}",
                String::from_utf8_lossy(attname)
            ),
        )
        .with_sqlstate(types_error::ERRCODE_INVALID_COLUMN_DEFINITION),
    )
}

// expand_planner_arrays + the parse->rtable append from
// expand_single_inheritance_child (inherit.c), fused per child.
fn add_child_rte<'mcx>(run: &mut PlannerRun<'mcx>, rte_node: Node<'mcx>) -> PgResult<u32> {
    let mcx = run.mcx;
    let parse = run.parse();
    // SAFETY: the sealed Query is exclusively planner-owned (interned by
    // subquery_planner from a planner-local copy); no other &mut aliases
    // exist and cell handles copied out earlier stay valid across the
    // cell-array regrow.
    let rtable = &parse.rtable as *const NodeList<'mcx> as *mut NodeList<'mcx>;
    unsafe { (*rtable).lappend(mcx, rte_node)? };
    let index = unsafe { (*rtable).len() as u32 - 1 };
    let rti = index + 1;
    run.root
        .simple_rte_array
        .push(types_pathnodes::RangeTblEntryId::Parse { query: run.root.parse, index });
    run.root.simple_rel_array.push(None);
    run.root.simple_rel_array_size = run.root.simple_rel_array.len() as i32;
    while run.root.append_rel_array.len() <= rti as usize {
        run.root.append_rel_array.push(None);
    }
    debug_assert_eq!(run.root.simple_rte_array.len() as u32, rti + 1);
    Ok(rti)
}

// The RestrictInfo form of adjust_appendrel_attrs_multilevel (appendinfo.c).
pub fn adjust_child_rinfo_multilevel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rid: types_pathnodes::RinfoId,
    childrel: RelId,
    parentrel: RelId,
) -> PgResult<types_pathnodes::RinfoId> {
    let mut rid = rid;
    let parent = run.root.rel(childrel).parent;
    if parent != Some(parentrel) {
        let up = parent.expect("childrel is not a child of parentrel");
        rid = adjust_child_rinfo_multilevel(run, rid, up, parentrel)?;
    }
    let relids = crate::relnode::relids_copy(run.mcx, &run.root.rel(childrel).relids);
    let appinfos = find_appinfos_by_relids(run, &relids);
    adjust_child_rinfo(run, rid, &appinfos)
}

// The expression form over a NodeId (interned) for multilevel translation.
pub fn adjust_child_expr_multilevel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    id: types_pathnodes::NodeId,
    childrel: RelId,
    parentrel: RelId,
) -> PgResult<types_pathnodes::NodeId> {
    let e = *run.root.expr_node(id);
    let tr = adjust_appendrel_attrs_multilevel(run, e, childrel, parentrel)?;
    Ok(run.intern_expr(tr))
}

// The expand_single_inheritance_child / distribute_row_identity_vars callers
// only run for DML, where preprocess_targetlist installed a planner-private
// tlist copy.
fn processed_tlist_append<'mcx>(run: &mut PlannerRun<'mcx>, tle: Node<'mcx>) -> PgResult<()> {
    let tl = run.processed_tlist.expect("processed_tlist set");
    // SAFETY: exclusively planner-owned (see above); regrowth never
    // invalidates previously handed-out cell nodes.
    let tl = tl as *const NodeList<'mcx> as *mut NodeList<'mcx>;
    unsafe { (*tl).lappend(run.mcx, tle)? };
    Ok(())
}

// add_row_identity_var (appendinfo.c).
pub fn add_row_identity_var<'mcx>(
    run: &mut PlannerRun<'mcx>,
    orig_var: Node<'mcx>,
    rtindex: u32,
    rowid_name: &'mcx str,
) -> PgResult<()> {
    let mcx = run.mcx;
    let v = orig_var.as_var().expect("Var");
    debug_assert_eq!(v.varno, rtindex as i32);
    debug_assert_eq!(v.varlevelsup, 0);
    debug_assert!(v.varnullingrels.is_empty());
    let (vartype, vartypmod) = (v.vartype, v.vartypmod);

    if rtindex as i32 == run.parse().resultRelation {
        let resno = run.processed_tlist.expect("processed_tlist set").len() as i16 + 1;
        let tle = Node::mk_target_entry(mcx, orig_var, resno, Some(rowid_name), true)?;
        return processed_tlist_append(run, tle);
    }

    debug_assert!(crate::relnode::relids_is_member(
        rtindex as i32,
        &run.root.leaf_result_relids
    ));
    debug_assert!(run.root.append_rel_array[rtindex as usize].is_some());

    let mut rowid_var = copy_var(mcx, v)?;
    rowid_var.varno = ROWID_VAR;
    let rowid_node = Node::mk(mcx, rowid_var)?;

    for i in 0..run.root.row_identity_vars.len() {
        let (name, var_id) = {
            let ri = &run.root.row_identity_vars[i];
            (ri.rowidname, ri.rowidvar)
        };
        if name != rowid_name {
            continue;
        }
        if types_nodes::equal(*run.root.expr_node(var_id), rowid_node) {
            let cur = crate::relnode::relids_take(&mut run.root.row_identity_vars[i].rowidrels);
            run.root.row_identity_vars[i].rowidrels =
                crate::relnode::relids_add_member(mcx, &cur, rtindex);
            return Ok(());
        }
        panic!("conflicting uses of row-identity name \"{rowid_name}\"");
    }

    let rowidvar = run.intern_expr(rowid_node);
    run.root.row_identity_vars.push(RowIdentityVarInfo {
        rowidvar,
        rowidwidth: lsyscache::get_typavgwidth(vartype, vartypmod)?,
        rowidname: rowid_name,
        rowidrels: crate::relnode::relids_singleton(mcx, rtindex),
    });

    let v = orig_var.as_var().expect("Var");
    let mut refvar = copy_var(mcx, v)?;
    refvar.varno = ROWID_VAR;
    refvar.varattno = run.root.row_identity_vars.len() as i16;
    let resno = run.processed_tlist.expect("processed_tlist set").len() as i16 + 1;
    let tle = Node::mk_target_entry(mcx, Node::mk(mcx, refvar)?, resno, Some(rowid_name), true)?;
    processed_tlist_append(run, tle)
}

// add_row_identity_columns (appendinfo.c). The FDW leg is C's default
// wholerow arm: no in-tree FDW installs AddForeignUpdateTargets.
pub fn add_row_identity_columns<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rtindex: u32,
    relkind: u8,
) -> PgResult<()> {
    let mcx = run.mcx;
    debug_assert!(matches!(
        run.parse().commandType,
        CmdType::CMD_UPDATE | CmdType::CMD_DELETE | CmdType::CMD_MERGE
    ));
    if relkind == types_rel::RELKIND_RELATION
        || relkind == types_rel::RELKIND_MATVIEW
        || relkind == types_rel::RELKIND_PARTITIONED_TABLE
    {
        let var = mk_var(
            mcx,
            rtindex,
            types_tuple::htup::SelfItemPointerAttributeNumber as i16,
            types_core::catalog::TIDOID,
            -1,
            0,
        )?;
        add_row_identity_var(run, var, rtindex, "ctid")?;
    } else if relkind == types_rel::RELKIND_FOREIGN_TABLE {
        let var = mk_var(mcx, rtindex, 0, types_core::catalog::RECORDOID, -1, 0)?;
        add_row_identity_var(run, var, rtindex, "wholerow")?;
    }
    Ok(())
}

// distribute_row_identity_vars (appendinfo.c).
pub fn distribute_row_identity_vars<'mcx>(run: &mut PlannerRun<'mcx>) -> PgResult<()> {
    let mcx = run.mcx;
    let parse = run.parse();
    if !matches!(
        parse.commandType,
        CmdType::CMD_UPDATE | CmdType::CMD_DELETE | CmdType::CMD_MERGE
    ) {
        debug_assert!(run.root.row_identity_vars.is_empty());
        return Ok(());
    }
    let result_relation = parse.resultRelation;
    let target_rte = parse
        .rtable
        .nth(result_relation as usize - 1)
        .as_range_tbl_entry()
        .expect("rtable cell");
    if !target_rte.inh {
        debug_assert!(run.root.row_identity_vars.is_empty());
        return Ok(());
    }

    if run.root.row_identity_vars.is_empty() {
        // Every leaf was excluded: fall back to the top rel's own identity
        // columns so the (never-executed) plan still carries junk columns.
        let rel = table::table_open(mcx, target_rte.relid, types_rel::NoLock)?;
        let relkind = rel.rd_rel.relkind;
        rel.close(types_rel::NoLock)?;
        add_row_identity_columns(run, result_relation as u32, relkind)?;
        crate::initsplan::build_base_rel_tlists(run)?;
        return Ok(());
    }

    let target_rel = run.root.simple_rel_array[result_relation as usize]
        .expect("target rel built");
    let tlist = run.processed_tlist.expect("processed_tlist set");
    let mut rowid_refs: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    for tle_node in tlist {
        let tle = tle_node.as_target_entry().expect("tlist cell");
        if let Some(var) = tle.expr.as_var() {
            if var.varno == ROWID_VAR {
                rowid_refs.push(Node::mk(mcx, copy_var(mcx, var)?)?);
            }
        }
    }
    for var in rowid_refs.iter() {
        let id = run.intern_expr(*var);
        run.root.rel_reltarget_mut(target_rel).exprs.push(id);
    }
    Ok(())
}

// translate_col_privs (inherit.c): attnums offset by
// FirstLowInvalidHeapAttributeNumber, whole-row expands to all inherited cols.
fn translate_col_privs<'mcx>(
    run: &PlannerRun<'mcx>,
    parent_privs: &types_nodes::Bitmapset<'mcx>,
    appinfo: &AppendRelInfo<'mcx>,
) -> PgResult<types_nodes::Bitmapset<'mcx>> {
    let mcx = run.mcx;
    const FLIHAN: i32 = types_tuple::htup::FirstLowInvalidHeapAttributeNumber;
    let mut child_privs = types_nodes::Bitmapset::empty();
    for attno in (FLIHAN + 1)..0 {
        if parent_privs.is_member(attno - FLIHAN) {
            child_privs.add_member(mcx, attno - FLIHAN)?;
        }
    }
    let whole_row = parent_privs.is_member(0 - FLIHAN);
    for (i, &tid) in appinfo.translated_vars.iter().enumerate() {
        let attno = i as i32 + 1;
        if tid == NodeId::default() {
            continue;
        }
        if whole_row || parent_privs.is_member(attno - FLIHAN) {
            let var = run.root.expr_node(tid).as_var().expect("translated var");
            child_privs.add_member(mcx, var.varattno as i32 - FLIHAN)?;
        }
    }
    Ok(child_privs)
}

// has_partition_attrs (catalog/partition.c), the planner-side copy (the
// tablecmds one is crate-private and the dep direction is wrong).
fn has_partition_attrs<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'mcx>,
    attnums: &types_nodes::Bitmapset<'mcx>,
) -> PgResult<bool> {
    if attnums.is_empty() || rel.rd_rel.relkind != types_rel::RELKIND_PARTITIONED_TABLE {
        return Ok(false);
    }
    const FLIHAN: i32 = types_tuple::htup::FirstLowInvalidHeapAttributeNumber;
    let key = partcache::RelationGetPartitionKey(rel)?;
    let mut partexprs_it = key.partexprs.iter();
    for i in 0..key.partnatts as usize {
        let partattno = key.partattrs[i];
        if partattno != 0 {
            if attnums.is_member(partattno as i32 - FLIHAN) {
                return Ok(true);
            }
        } else {
            let expr = partexprs_it.next().expect("partition key expression");
            let mut expr_attrs = types_nodes::Bitmapset::empty();
            vars::pull_varattnos(mcx, expr, 1, &mut expr_attrs)?;
            if attnums.overlap(&expr_attrs) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

// get_translated_update_targetlist (appendinfo.c); FDW deparse consumer, kept
// for the executor lane's contract.
#[allow(dead_code)]
pub fn get_translated_update_targetlist<'mcx>(
    run: &mut PlannerRun<'mcx>,
    relid: u32,
) -> PgResult<(NodeList<'mcx>, PgVec<'mcx, i16>)> {
    let mcx = run.mcx;
    debug_assert!(run.parse().commandType == CmdType::CMD_UPDATE);
    let result_relation = run.parse().resultRelation as u32;
    if relid == result_relation {
        let tl = run.processed_tlist.expect("processed_tlist set").clone_in(mcx)?;
        let colnos = crate::relnode::pgvec_clone_shallow(mcx, &run.root.update_colnos);
        return Ok((tl, colnos));
    }
    debug_assert!(crate::relnode::relids_is_member(
        relid as i32,
        &run.root.all_result_relids
    ));
    let childrel = run.root.simple_rel_array[relid as usize].expect("child rel built");
    let toprel =
        run.root.simple_rel_array[result_relation as usize].expect("target rel built");
    let src = run.processed_tlist.expect("processed_tlist set").clone_in(mcx)?;
    let mut tl = NodeList::nil();
    for tle in &src {
        tl.lappend(mcx, adjust_appendrel_attrs_multilevel(run, tle, childrel, toprel)?)?;
    }
    let colnos_src = crate::relnode::pgvec_clone_shallow(mcx, &run.root.update_colnos);
    let colnos =
        adjust_inherited_attnums_multilevel(run, colnos_src.as_slice(), relid, result_relation);
    Ok((tl, colnos))
}
