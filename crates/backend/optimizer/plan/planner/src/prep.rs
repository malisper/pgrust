use mcx::{alloc_leak_in, Mcx};
use types_error::PgResult;
use types_nodes::list::NodeList;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{Query, RTEKind, RangeTblEntry};
use types_nodes::primnodes::{Alias, FromExpr};
use types_nodes::{Node, NodeTag};

use crate::run::PlannerRun;

// Empty FROM becomes a dummy RTE_RESULT + RangeTblRef. C mutates the FromExpr
// in place; jointree is a shared ref here, so an equivalent one is rebuilt.
pub fn replace_empty_jointree<'mcx>(mcx: Mcx<'mcx>, parse: &mut Query<'mcx>) -> PgResult<()> {
    let quals = match parse.jointree {
        Some(f) if f.fromlist.is_nil() => f.quals,
        Some(_) => return Ok(()),
        None => None,
    };
    if parse.setOperations.is_some() {
        return Ok(());
    }

    let eref = alloc_leak_in(
        mcx,
        Alias { aliasname: Some("*RESULT*"), colnames: NodeList::nil() },
    )?;
    let mut rte = Node::build::<RangeTblEntry>(mcx)?;
    rte.rtekind = RTEKind::RTE_RESULT;
    rte.eref = Some(eref);
    parse.rtable.lappend(mcx, rte.seal())?;
    let rti = parse.rtable.len() as i32;

    let rtr = Node::mk_range_tbl_ref(mcx, rti)?;
    let fromlist = NodeList::make1(mcx, rtr)?;
    parse.jointree = Some(alloc_leak_in(mcx, FromExpr { fromlist, quals })?);
    Ok(())
}

// A lone RangeTblRef under the top FromExpr can never be elided or dropped.
pub fn remove_useless_result_rtes(run: &PlannerRun<'_>, parse: &Query<'_>) {
    let f = parse.jointree.expect("top jointree is a FromExpr");
    if f.fromlist.len() == 1
        && f.fromlist.nth(0).node_tag() == NodeTag::T_RangeTblRef
    {
        debug_assert!(run.root.rowMarks.is_empty());
        return;
    }
    panic!(
        "remove_useless_results_recurse (prepjointree.c): non-trivial jointree; \
         M2 join lane"
    );
}

// SELECT without locking clauses needs no rowmarks.
pub fn preprocess_rowmarks(parse: &Query<'_>) {
    if !parse.rowMarks.is_nil() {
        panic!("preprocess_rowmarks (planner.c): FOR UPDATE/SHARE rowmarks; M2 lane");
    }
    if !matches!(parse.commandType, CmdType::CMD_SELECT | CmdType::CMD_INSERT) {
        panic!("preprocess_rowmarks (planner.c): UPDATE/DELETE/MERGE rowmarks; M2 DML lane");
    }
}

// SELECT arm shares the parse targetList (as C); the INSERT arm NULL-fills
// missing columns (expand_insert_targetlist). UPDATE/DELETE/MERGE row-identity
// lanes are loud.
pub fn preprocess_targetlist<'mcx>(run: &mut PlannerRun<'mcx>) -> PgResult<()> {
    let mcx = run.mcx;
    let parse = run.parse();
    debug_assert!(run.root.rowMarks.is_empty());
    if parse.resultRelation == 0 {
        debug_assert!(parse.commandType == CmdType::CMD_SELECT);
        run.processed_tlist = Some(&parse.targetList);
        return Ok(());
    }
    let command_type = parse.commandType;
    if command_type != CmdType::CMD_INSERT {
        panic!(
            "preprocess_targetlist (preptlist.c): UPDATE/DELETE/MERGE row-identity \
             lane (rewriteTargetListUD/add_row_identity_var); M4 DML lane"
        );
    }
    let rte = parse
        .rtable
        .nth(parse.resultRelation as usize - 1)
        .as_range_tbl_entry()
        .expect("rtable cell");
    debug_assert!(rte.rtekind == RTEKind::RTE_RELATION);
    let rel = table::table_open(mcx, rte.relid, types_rel::NoLock)?;
    let tlist = expand_insert_targetlist(mcx, &parse.targetList, &rel)?;
    table::table_close(rel, types_rel::NoLock)?;
    debug_assert!(parse.returningList.is_nil());
    run.processed_tlist = Some(mcx::leak_in(mcx::alloc_in(mcx, tlist)?));
    Ok(())
}

// expand_insert_targetlist (preptlist.c): produce one entry per attribute in
// attno order, NULL Consts for unassigned columns. Domain columns need
// coerce_null_to_domain's CoerceToDomain wrapper — loud.
fn expand_insert_targetlist<'mcx>(
    mcx: Mcx<'mcx>,
    tlist: &NodeList<'mcx>,
    rel: &types_rel::Relation<'mcx>,
) -> PgResult<NodeList<'mcx>> {
    let mut new_tlist = NodeList::nil();
    let mut tlist_iter = tlist.iter().peekable();
    let numattrs = rel.rd_att.natts;
    for attrno in 1..=numattrs {
        let att = rel.rd_att.attr(attrno as usize - 1);
        let mut new_tle = None;
        if let Some(&tle_node) = tlist_iter.peek() {
            let tle = tle_node.as_target_entry().expect("tlist cell");
            if !tle.resjunk && tle.resno == attrno as i16 {
                new_tle = Some(tle_node);
                tlist_iter.next();
            }
        }
        let tle_node = match new_tle {
            Some(t) => t,
            None => {
                let new_expr = if !att.attisdropped {
                    debug_assert!(att.attgenerated == 0);
                    if lsyscache::typ::getBaseType(att.atttypid)? != att.atttypid {
                        panic!(
                            "expand_insert_targetlist (preptlist.c): \
                             coerce_null_to_domain (CoerceToDomain wrapper); M4 domain lane"
                        );
                    }
                    Node::mk_const(
                        mcx,
                        att.atttypid,
                        att.atttypmod,
                        att.attcollation,
                        att.attlen as i32,
                        datum::Datum::null(),
                        true,
                        att.attbyval,
                    )?
                } else {
                    Node::mk_const(mcx, 23, -1, 0, 4, datum::Datum::null(), true, true)?
                };
                let name =
                    core::str::from_utf8(att.attname.name_str()).expect("attname is UTF-8");
                let name = mcx::slice_borrow_in(mcx, name.as_bytes())?;
                // SAFETY: byte-for-byte copy of a &str.
                let name = unsafe { core::str::from_utf8_unchecked(name) };
                Node::mk_target_entry(mcx, new_expr, attrno as i16, Some(name), false)?
            }
        };
        new_tlist.lappend(mcx, tle_node)?;
    }
    for tle_node in tlist_iter {
        let tle = tle_node.as_target_entry().expect("tlist cell");
        assert!(tle.resjunk, "targetlist is not sorted correctly");
        panic!(
            "expand_insert_targetlist (preptlist.c): junk tlist entries; M4 lane"
        );
    }
    Ok(new_tlist)
}
