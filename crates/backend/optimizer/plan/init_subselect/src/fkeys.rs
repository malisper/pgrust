//! FOREIGN KEYS (initsplan.c) — `match_foreign_keys_to_quals`.
//!
//! Matches the foreign-key constraints in `root->fkey_list` against the query's
//! equivalence classes and loose join quals, annotating each
//! [`::pathnodes::ForeignKeyOptInfo`] with which ECs/RestrictInfos its
//! columns match, and discarding FKs that aren't fully matched. The match
//! results feed the FK-based join-selectivity estimates in costsize.c.

extern crate alloc;

use alloc::vec::Vec;
use ::equivclass::match_eclasses_to_foreign_key_col;
use ::types_core::{Oid, OidIsValid, InvalidOid};
use ::nodes::primnodes::{Expr, Var};
use ::pathnodes::{NodeId, PlannerInfo, RELOPT_BASEREL};

/// `match_foreign_keys_to_quals` (initsplan.c:3631).
///
/// Annotates each `root->fkey_list` `ForeignKeyOptInfo` with the ECs and loose
/// join quals matching its columns, then rebuilds `root->fkey_list` keeping only
/// the fully-matched FKs.
pub fn match_foreign_keys_to_quals(root: &mut PlannerInfo) {
    let mut newlist: Vec<NodeId> = Vec::new();

    // foreach(lc, root->fkey_list)
    let fkey_list = root.fkey_list.clone();
    for fk_id in fkey_list {
        // Pull the FK out of the arena so we can mutate it while reading `root`
        // immutably (C mutates the fkinfo in place; the arena split forces a
        // take-modify-store dance). Reads/writes below match the C 1:1.
        let mut fkinfo = root.foreign_key(fk_id).clone();

        // Either relid might identify a rel that is in the query's rtable but
        // isn't referenced by the jointree, or has been removed by join
        // removal, so that it won't have a RelOptInfo. Hence don't use
        // find_base_rel() here. We can ignore such FKs.
        if fkinfo.con_relid as i32 >= root.simple_rel_array_size
            || fkinfo.ref_relid as i32 >= root.simple_rel_array_size
        {
            continue; // just paranoia
        }
        let con_rel = match root.simple_rel_array[fkinfo.con_relid as usize] {
            Some(r) => r,
            None => continue,
        };
        let ref_rel = match root.simple_rel_array[fkinfo.ref_relid as usize] {
            Some(r) => r,
            None => continue,
        };

        // Ignore FK unless both rels are baserels. This gets rid of FKs that
        // link to inheritance child rels (otherrels).
        if root.rel(con_rel).reloptkind != RELOPT_BASEREL
            || root.rel(ref_rel).reloptkind != RELOPT_BASEREL
        {
            continue;
        }

        // Scan the columns and try to match them to eclasses and quals.
        //
        // Note: for simple inner joins, any match should be in an eclass.
        // "Loose" quals that syntactically match an FK equality must have been
        // rejected for EC status because they are outer-join quals or similar.
        // We can still consider them to match the FK.
        for colno in 0..fkinfo.nkeys as usize {
            let ec = match_eclasses_to_foreign_key_col(root, &mut fkinfo, colno);
            // Don't bother looking for loose quals if we got an EC match.
            if let Some(ec_id) = ec {
                fkinfo.nmatched_ec += 1;
                if root.ec(ec_id).ec_has_const {
                    fkinfo.nconst_ec += 1;
                }
                continue;
            }

            // Scan joininfo list for relevant clauses. Either rel's joininfo
            // list would do equally well; we use con_rel's.
            let con_attno = fkinfo.conkey[colno];
            let ref_attno = fkinfo.confkey[colno];
            let mut fpeqop: Oid = InvalidOid; // we'll look this up only if needed

            let joininfo = root.rel(con_rel).joininfo.clone();
            for rinfo in joininfo {
                let clause = root.node(root.rinfo(rinfo).clause);

                // Only binary OpExprs are useful for consideration.
                let op = match clause.as_opexpr() {
                    Some(o) if o.args.len() == 2 => o,
                    _ => continue,
                };
                // get_leftop / get_rightop, then strip RelabelType wrappers.
                let leftvar = strip_relabeltypes(&op.args[0]);
                let rightvar = strip_relabeltypes(&op.args[1]);

                // Operands must be Vars, possibly with RelabelType.
                let leftvar: &Var = match leftvar.and_then(Expr::as_var) {
                    Some(v) => v,
                    None => continue,
                };
                let rightvar: &Var = match rightvar.and_then(Expr::as_var) {
                    Some(v) => v,
                    None => continue,
                };

                // Now try to match the vars to the current foreign key cols.
                if fkinfo.ref_relid as i32 == leftvar.varno
                    && ref_attno == leftvar.varattno
                    && fkinfo.con_relid as i32 == rightvar.varno
                    && con_attno == rightvar.varattno
                {
                    // Vars match, but is it the right operator?
                    if op.opno == fkinfo.conpfeqop[colno] {
                        fkinfo.rinfos[colno].push(rinfo);
                        fkinfo.nmatched_ri += 1;
                    }
                } else if fkinfo.ref_relid as i32 == rightvar.varno
                    && ref_attno == rightvar.varattno
                    && fkinfo.con_relid as i32 == leftvar.varno
                    && con_attno == leftvar.varattno
                {
                    // Reverse match, must check commutator operator. Look it up
                    // if we didn't already. (In the worst case we might do
                    // multiple lookups here, but that would require an FK
                    // equality operator without commutator, which is unlikely.)
                    if !OidIsValid(fpeqop) {
                        fpeqop = lsyscache_seams::get_commutator::call(
                            fkinfo.conpfeqop[colno],
                        )
                        .expect("match_foreign_keys_to_quals: get_commutator");
                    }
                    if op.opno == fpeqop {
                        fkinfo.rinfos[colno].push(rinfo);
                        fkinfo.nmatched_ri += 1;
                    }
                }
            }
            // If we found any matching loose quals, count col as matched.
            if !fkinfo.rinfos[colno].is_empty() {
                fkinfo.nmatched_rcols += 1;
            }
        }

        // Currently, we drop multicolumn FKs that aren't fully matched to the
        // query. Later we might figure out how to derive some sort of estimate
        // from them, in which case this test should be weakened to
        // "if ((fkinfo->nmatched_ec + fkinfo->nmatched_rcols) > 0)".
        let fully_matched = (fkinfo.nmatched_ec + fkinfo.nmatched_rcols) == fkinfo.nkeys;

        // Store the annotated FK back into the arena.
        *root.foreign_key_mut(fk_id) = fkinfo;

        if fully_matched {
            newlist.push(fk_id);
        }
    }

    // Replace fkey_list, thereby discarding any useless entries.
    root.fkey_list = newlist;
}

/// `while (var && IsA(var, RelabelType)) var = ((RelabelType *) var)->arg;`
///
/// Strips zero or more `RelabelType` wrappers; returns the innermost `Expr`
/// (or `None` if a RelabelType had a NULL `arg`, mirroring the C loop exiting
/// on a NULL pointer).
fn strip_relabeltypes<'a, 'mcx>(mut expr: &'a Expr<'mcx>) -> Option<&'a Expr<'mcx>> {
    loop {
        match expr.as_relabeltype() {
            Some(r) => match r.arg.as_deref() {
                Some(inner) => expr = inner,
                None => return None,
            },
            None => return Some(expr),
        }
    }
}
