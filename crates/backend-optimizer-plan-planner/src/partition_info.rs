//! `set_relation_partition_info` / `set_baserel_partition_constraint`
//! (plancat.c) — the partitioning-scheme/bounds/key-expr/qual fill for an
//! inheritance-parent partitioned `RelOptInfo`.
//!
//! These functions are `static` in plancat.c, so by crate correspondence their
//! home is `backend-optimizer-util-plancat`. But the bodies span
//! `RelationGetPartitionKey`/`RelationGetPartitionQual` (partcache),
//! `CreatePartitionDirectory`/`PartitionDirectoryLookup` (partdesc),
//! `expression_planner` (this crate), `fmgr_info_copy` (fmgr) and `copyObject`
//! — substrate that plancat cannot depend on without a dependency cycle
//! (`planner -> plancat`). The planner sits above plancat and already owns
//! `expression_planner`, so it installs the two `*-ext-seams` here. plancat
//! still *calls* the seams (get_relation_info / get_relation_constraints); the
//! direction is `consumer plancat -> ext-seam <- owner planner`, no cycle.

use alloc::boxed::Box;
use alloc::vec::Vec;

use mcx::Mcx;
use types_core::primitive::{AttrNumber, Index, InvalidAttrNumber, Oid};
use types_error::{PgError, PgResult};
use types_nodes::primnodes::Expr;
use types_pathnodes::{NodeId, PartitionBoundInfoData, PartitionScheme, PartitionSchemeData, PlannerInfo, RelId};
use types_storage::lock::NoLock;

use backend_optimizer_util_plancat_ext_seams as plancat_ext;

/// Install the two partitioning ext-seams owned by this crate.
pub(crate) fn init_seams() {
    plancat_ext::set_relation_partition_info::set(set_relation_partition_info);
    plancat_ext::set_baserel_partition_constraint::set(set_baserel_partition_constraint);
}

/// `set_relation_partition_info(root, rel, relation)` (plancat.c:2455).
///
/// Sets the partitioning scheme and related information for a partitioned
/// table. The seam carries `relid` rather than the open `Relation` (the C
/// caller still holds it open under its lock); re-open by OID with `NoLock`
/// (lock already held by `get_relation_info`), read everything the C reads off
/// the relcache entry, and close before returning.
fn set_relation_partition_info(
    root: &mut PlannerInfo,
    rel: RelId,
    relid: Oid,
) -> PgResult<()> {
    // A transient context for the relcache open + partition-key/desc reads, the
    // analogue of `get_relation_info`'s `relcx`. Everything that must outlive
    // this call (partexprs/partition_qual nodes, the PartitionScheme, the
    // PartitionDirectory's pinned descriptor) is interned into a lifetime-free
    // store (`root.node_arena`, `root.part_schemes`, `glob.partition_directory`)
    // before the context drops.
    let relcx = mcx::MemoryContext::new("set_relation_partition_info");
    let mcx = relcx.mcx();

    // We need not lock the relation since it was already locked by the caller.
    let relation =
        backend_access_common_relation_seams::relation_open::call(mcx, relid, NoLock)?;

    // Create the PartitionDirectory infrastructure if we didn't already.
    //
    //   if (root->glob->partition_directory == NULL)
    //       root->glob->partition_directory =
    //           CreatePartitionDirectory(CurrentMemoryContext, true);
    {
        let glob = root
            .glob
            .as_mut()
            .expect("set_relation_partition_info: root->glob is NULL");
        if glob.partition_directory.0.is_none() {
            glob.partition_directory =
                backend_partitioning_core_seams::create_partition_directory::call(mcx, true)?;
        }
    }

    // partdesc = PartitionDirectoryLookup(root->glob->partition_directory, relation);
    let partdesc = {
        let glob = root.glob.as_mut().unwrap();
        backend_partitioning_core_seams::partition_directory_lookup::call(
            mcx,
            &mut glob.partition_directory,
            relation.alias(),
        )?
    };

    // rel->part_scheme = find_partition_scheme(root, relation);
    let part_scheme = find_partition_scheme(mcx, root, &relation)?;

    // Assert(partdesc != NULL && rel->part_scheme != NULL);
    debug_assert!(part_scheme.is_some());

    // rel->boundinfo = partdesc->boundinfo;
    //
    // The consumer-layer `PartitionBoundInfoData` is opaque (the bound algebra
    // lives with partbounds); mirror C's pointer copy by carrying presence: a
    // partitioned table with a partdesc always has boundinfo.
    let has_boundinfo = partdesc.boundinfo.is_some();
    // rel->nparts = partdesc->nparts;
    let nparts = partdesc.nparts;

    {
        let r = root.rel_mut(rel);
        r.part_scheme = part_scheme;
        r.boundinfo = if has_boundinfo {
            Some(Box::new(PartitionBoundInfoData {}))
        } else {
            None
        };
        r.nparts = nparts;
    }

    // set_baserel_partition_key_exprs(relation, rel);
    set_baserel_partition_key_exprs(mcx, root, rel, &relation)?;

    // set_baserel_partition_constraint(relation, rel);
    set_baserel_partition_constraint_inner(mcx, root, rel, &relation)?;

    // table_close(relation, NoLock) — the open above was a fresh pin; release it.
    relation.close(NoLock)?;

    Ok(())
}

/// `find_partition_scheme(root, relation)` (plancat.c:2485) — find or create a
/// `PartitionScheme` for this relation. Returns an owned clone of the matched
/// (or freshly created and appended) scheme to store on the `RelOptInfo`
/// (`PartitionScheme = Option<Box<PartitionSchemeData>>`; the C shared pointer
/// is modelled as an owned value the planner never frees mid-run).
fn find_partition_scheme<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    relation: &types_rel::Relation<'mcx>,
) -> PgResult<PartitionScheme> {
    // PartitionKey partkey = RelationGetPartitionKey(relation);
    let partkey = backend_utils_cache_partcache::RelationGetPartitionKey(mcx, relation)?
        .ok_or_else(|| {
            // A partitioned table should have a partition key.
            PgError::error("find_partition_scheme: partitioned table has no partition key")
        })?;

    let partnatts = partkey.partnatts as usize;

    // Search for a matching partition scheme and return if found one.
    //
    //   foreach(lc, root->part_schemes) { ... }
    //
    // Match: strategy, partnatts, and the per-column partopfamily/partopcintype/
    // partcollation arrays. (parttyplen/parttypbyval are Asserted equal when
    // partopcintype matches; partsupfunc OIDs likewise — `PartitionSchemeData`'s
    // hand-written `PartialEq` already compares partsupfunc by `fn_oid`.)
    let strategy = partkey.strategy as i8;
    for existing in root.part_schemes.iter() {
        let Some(ps) = existing.as_ref() else {
            continue;
        };
        if strategy != ps.strategy || partkey.partnatts != ps.partnatts {
            continue;
        }
        if partkey.partopfamily.as_slice()[..partnatts] != ps.partopfamily[..partnatts]
            || partkey.partopcintype.as_slice()[..partnatts] != ps.partopcintype[..partnatts]
            || partkey.partcollation.as_slice()[..partnatts] != ps.partcollation[..partnatts]
        {
            continue;
        }
        // Found matching partition scheme — return an owned copy.
        return Ok(Some(Box::new(PartitionSchemeData::clone(ps))));
    }

    // Did not find a matching partition scheme. Create one, copying the relevant
    // information from the relcache (we copy the array contents since the
    // relcache entry may not survive after we close the relation).
    let mut partsupfunc: Vec<types_core::fmgr::FmgrInfo> = Vec::with_capacity(partnatts);
    for i in 0..partnatts {
        // fmgr_info_copy(&part_scheme->partsupfunc[i], &partkey->partsupfunc[i], cxt).
        // The C `fmgr_info_copy` is `*dstinfo = *srcinfo; dstinfo->fn_extra = NULL`
        // — a flat copy that clears the opaque per-call cache. This `FmgrInfo`
        // (`types_core::fmgr`) carries no `fn_extra` field (the cache is not
        // modelled), so the clone IS the faithful copy.
        partsupfunc.push(partkey.partsupfunc.as_slice()[i].clone());
    }

    let part_scheme = PartitionSchemeData {
        strategy,
        partnatts: partkey.partnatts,
        partopfamily: partkey.partopfamily.as_slice()[..partnatts].to_vec(),
        partopcintype: partkey.partopcintype.as_slice()[..partnatts].to_vec(),
        partcollation: partkey.partcollation.as_slice()[..partnatts].to_vec(),
        parttyplen: partkey.parttyplen.as_slice()[..partnatts].to_vec(),
        parttypbyval: partkey.parttypbyval.as_slice()[..partnatts].to_vec(),
        partsupfunc,
    };

    // Add the partitioning scheme to PlannerInfo (root->part_schemes), and
    // return an owned copy for the RelOptInfo.
    let stored = Box::new(part_scheme.clone());
    root.part_schemes.push(Some(Box::new(part_scheme)));
    Ok(Some(stored))
}

/// `set_baserel_partition_key_exprs(relation, rel)` (plancat.c:2592) — build
/// `rel->partexprs` (and allocate the empty `rel->nullable_partexprs`).
fn set_baserel_partition_key_exprs<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
    relation: &types_rel::Relation<'mcx>,
) -> PgResult<()> {
    // PartitionKey partkey = RelationGetPartitionKey(relation);
    let partkey = backend_utils_cache_partcache::RelationGetPartitionKey(mcx, relation)?
        .ok_or_else(|| {
            PgError::error("set_baserel_partition_key_exprs: partitioned table has no partition key")
        })?;

    // Index varno = rel->relid; Assert(IS_SIMPLE_REL(rel) && rel->relid > 0);
    let varno: Index = root.rel(rel).relid;
    debug_assert!(varno > 0);

    let partnatts = partkey.partnatts as usize;

    // The single-expr-per-key result lists, one per partitioning column, built as
    // arena `NodeId` handles. Expression keys (attno == 0) are copied + re-stamped
    // through `ChangeVarNodes`; the re-stamp runs over the already-interned arena
    // handle via the `change_var_nodes` seam.
    let mut partexprs: Vec<Vec<NodeId>> = Vec::with_capacity(partnatts);

    // lc = list_head(partkey->partexprs);
    let mut expr_idx = 0usize;

    for cnt in 0..partnatts {
        // AttrNumber attno = partkey->partattrs[cnt];
        let attno: AttrNumber = partkey.partattrs.as_slice()[cnt];

        let partexpr_id: NodeId = if attno != InvalidAttrNumber {
            // Single column partition key is stored as a Var node.
            //   partexpr = makeVar(varno, attno, parttypid, parttypmod, parttypcoll, 0);
            debug_assert!(attno > 0);
            let var = backend_nodes_core::makefuncs::make_var(
                varno as i32,
                attno,
                partkey.parttypid.as_slice()[cnt],
                partkey.parttypmod.as_slice()[cnt],
                partkey.parttypcoll.as_slice()[cnt],
                0,
            );
            root.alloc_node(Expr::Var(var))
        } else {
            // Expression key: partexpr = copyObject(lfirst(lc));
            //                 ChangeVarNodes((Node *) partexpr, 1, varno, 0);
            let src = partkey.partexprs.as_slice().get(expr_idx).ok_or_else(|| {
                PgError::error("wrong number of partition key expressions")
            })?;
            // copyObject: clone the relcache expr into the lifetime-free arena.
            let id = root.alloc_node(src.clone());
            // Re-stamp the expression with the given varno (relid 1 -> varno).
            plancat_ext::change_var_nodes::call(root, &[id], 1, varno as i32);
            expr_idx += 1;
            id
        };

        // Base relations have a single expression per key: partexprs[cnt] = list_make1(partexpr).
        partexprs.push(alloc::vec![partexpr_id]);
    }

    // rel->partexprs = partexprs;
    // rel->nullable_partexprs = palloc0(sizeof(List *) * partnatts) — partnatts
    // empty lists (a base rel has no nullable partition key exprs).
    {
        let r = root.rel_mut(rel);
        r.partexprs = partexprs;
        r.nullable_partexprs = alloc::vec![Vec::new(); partnatts];
    }

    Ok(())
}

/// `set_baserel_partition_constraint(relation, rel)` (plancat.c:2660) — the
/// installed ext-seam form (re-opens the relation by OID, since the seam does
/// not carry the open handle). Used by `get_relation_constraints` when
/// `include_partition` and the rel is a partition.
fn set_baserel_partition_constraint(root: &mut PlannerInfo, rel: RelId, relid: Oid) -> PgResult<()> {
    // if (rel->partition_qual) /* already done */ return;
    if !root.rel(rel).partition_qual.is_empty() {
        return Ok(());
    }

    let relcx = mcx::MemoryContext::new("set_baserel_partition_constraint");
    let mcx = relcx.mcx();

    let relation =
        backend_access_common_relation_seams::relation_open::call(mcx, relid, NoLock)?;

    set_baserel_partition_constraint_inner(mcx, root, rel, &relation)?;

    relation.close(NoLock)?;
    Ok(())
}

/// Shared body of `set_baserel_partition_constraint` (plancat.c:2660) over an
/// already-open relation — called both from `set_relation_partition_info` (which
/// holds the relation open) and the standalone ext-seam.
fn set_baserel_partition_constraint_inner<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
    relation: &types_rel::Relation<'mcx>,
) -> PgResult<()> {
    // if (rel->partition_qual) /* already done */ return;
    if !root.rel(rel).partition_qual.is_empty() {
        return Ok(());
    }

    // partconstr = RelationGetPartitionQual(relation);
    let partconstr = backend_utils_cache_partcache::RelationGetPartitionQual(mcx, relation)?;
    if partconstr.is_empty() {
        return Ok(());
    }

    // partconstr = (List *) expression_planner((Expr *) partconstr);
    //
    // The qual is an implicit-AND `List` of independent quals; const-fold each
    // element (the planner-local expression_planner takes a single Expr) and
    // intern it into the arena. `expression_planner` is `super::expression_planner`.
    let varno: Index = root.rel(rel).relid;
    let mut qual_ids: Vec<NodeId> = Vec::with_capacity(partconstr.len());
    for node in partconstr.into_iter() {
        let expr = node
            .into_expr()
            .ok_or_else(|| PgError::error("set_baserel_partition_constraint: qual is not an Expr"))?;
        let folded = super::expression_planner(mcx, expr)?;
        let id = root.alloc_node(folded);
        qual_ids.push(id);
    }

    // if (rel->relid != 1) ChangeVarNodes((Node *) partconstr, 1, rel->relid, 0);
    if varno != 1 {
        plancat_ext::change_var_nodes::call(root, &qual_ids, 1, varno as i32);
    }

    // rel->partition_qual = partconstr;
    root.rel_mut(rel).partition_qual = qual_ids;

    Ok(())
}
