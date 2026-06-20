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
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_optimizer_util_plancat_ext_seams as plancat_ext;

/// Install the partitioning + extended-statistics ext-seams owned by this crate.
pub(crate) fn init_seams() {
    plancat_ext::set_relation_partition_info::set(set_relation_partition_info);
    plancat_ext::set_baserel_partition_constraint::set(set_baserel_partition_constraint);
    plancat_ext::process_check_constraint::set(process_check_constraint);
    plancat_ext::get_stat_ext_keys_exprs::set(get_stat_ext_keys_exprs);
    plancat_ext::get_stat_ext_data_kinds::set(get_stat_ext_data_kinds);
}

/// `get_relation_statistics`'s per-stat-object key/expression preprocessing
/// (plancat.c:1522-1580) for one `pg_statistic_ext` row (`statOid`):
///
/// ```text
/// htup = SearchSysCache1(STATEXTOID, statOid);   -- elog if missing
/// for (i = 0; i < staForm->stxkeys.dim1; i++)
///     keys = bms_add_member(keys, staForm->stxkeys.values[i]);
/// datum = SysCacheGetAttr(.., Anum_pg_statistic_ext_stxexprs, &isnull);
/// if (!isnull) {
///     exprs = (List *) stringToNode(TextDatumGetCString(datum));
///     exprs = (List *) eval_const_expressions(NULL, (Node *) exprs);
///     fix_opfuncids((Node *) exprs);
///     if (varno != 1) ChangeVarNodes((Node *) exprs, 1, varno, 0);
/// }
/// ```
///
/// Returns the covered-column attnums and the decoded/const-folded expression
/// list interned into the planner (`root`) arena as `NodeId` handles, with Vars
/// re-stamped to the parent relation's varno. The catalog read (stxkeys +
/// raw stxexprs text) is the syscache projection; the node-vocabulary transforms
/// (which plancat cannot reach without a `planner -> plancat` cycle) run here.
fn get_stat_ext_keys_exprs(
    root: &mut PlannerInfo,
    stat_oid: Oid,
    varno: i32,
) -> PgResult<(Vec<i32>, Vec<NodeId>)> {
    // htup = SearchSysCache1(STATEXTOID, statOid);
    // if (!HeapTupleIsValid(htup)) elog(ERROR, "cache lookup failed ...");
    let scratch = mcx::MemoryContext::new("get_stat_ext_keys_exprs");
    let mcx = scratch.mcx();
    let (keys, exprs_text) =
        backend_utils_cache_syscache_seams::statext_keys_exprs_text::call(mcx, stat_oid)?
            .ok_or_else(|| {
                PgError::error(alloc::format!(
                    "cache lookup failed for statistics object {stat_oid}"
                ))
            })?;

    // No stxexprs ⇒ NIL expression list (the common column-only stat object).
    let Some(exprs_text) = exprs_text else {
        return Ok((keys, Vec::new()));
    };

    // exprs = (List *) stringToNode(exprsString);
    let node = backend_nodes_read_seams::string_to_node::call(mcx, exprs_text.as_str())?;
    let elems = mcx::PgBox::into_inner(node).into_list().ok_or_else(|| {
        PgError::error("get_stat_ext_keys_exprs: stxexprs stringToNode did not yield a List")
    })?;

    // exprs = (List *) eval_const_expressions(NULL, (Node *) exprs);  (root-less:
    // stat expressions, like CHECK constraints, contain no subqueries, so the
    // Param/sublink leg of eval_const_expressions never runs.)
    // fix_opfuncids((Node *) exprs);  — per element, as fix_opfuncids walks the tree.
    let mut ids: Vec<NodeId> = Vec::with_capacity(elems.len());
    for el in elems.into_iter() {
        let expr = mcx::PgBox::into_inner(el).into_expr().ok_or_else(|| {
            PgError::error("get_stat_ext_keys_exprs: stxexprs element is not an Expr")
        })?;
        let mut folded =
            backend_optimizer_plan_init_subselect_ext_seams::eval_const_expressions_expr::call(
                mcx, expr,
            )?;
        backend_nodes_core::nodefuncs::fix_opfuncids(&mut folded)?;
        ids.push(root.alloc_node(folded));
    }

    // if (varno != 1) ChangeVarNodes((Node *) exprs, 1, varno, 0);  — restamp the
    // Vars, which the catalog stores with varno == 1, to the parent relation.
    if varno != 1 {
        plancat_ext::change_var_nodes::call(root, &ids, 1, varno);
    }

    Ok((keys, ids))
}

/// `get_relation_statistics_worker`'s data-row read (plancat.c:1428-1490) for one
/// `(statOid, inh)` pair: returns the `stxdinherit` flag and which statistics
/// kinds are built (in the fixed `STATS_EXT_*` order NDISTINCT, DEPENDENCIES,
/// MCV, EXPRESSIONS) via `statext_is_kind_built` (the non-null status of each
/// kind's `pg_statistic_ext_data` column), or `None` when no data row exists.
fn get_stat_ext_data_kinds(
    stat_oid: Oid,
    inh: bool,
) -> PgResult<Option<plancat_ext::StatExtDataKinds>> {
    let Some((stxdinherit, built)) =
        backend_utils_cache_syscache_seams::statext_data_built_kinds::call(stat_oid, inh)?
    else {
        return Ok(None);
    };

    // Fixed order: NDISTINCT, DEPENDENCIES, MCV, EXPRESSIONS — the StatisticExtInfo
    // `char kind` per built statistic, matching the C worker's four if-blocks.
    let kind_chars = [
        types_statistics::STATS_EXT_NDISTINCT,
        types_statistics::STATS_EXT_DEPENDENCIES,
        types_statistics::STATS_EXT_MCV,
        types_statistics::STATS_EXT_EXPRESSIONS,
    ];
    let mut kinds: Vec<i8> = Vec::new();
    for (i, &is_built) in built.iter().enumerate() {
        if is_built {
            kinds.push(kind_chars[i]);
        }
    }

    Ok(Some(plancat_ext::StatExtDataKinds {
        stxdinherit,
        kinds,
    }))
}

/// `get_relation_constraints`'s per-check-constraint body (plancat.c:1305) over a
/// single validated CHECK constraint's `pg_constraint.ccbin` text. The caller
/// (`get_relation_constraints` in plancat) has already applied the `ccvalid` and
/// (NO INHERIT vs `include_noinherit`) filters; this performs the node-vocabulary
/// transforms plancat cannot reach without a `planner -> plancat` cycle:
///
/// ```text
/// cexpr = stringToNode(ccbin);
/// cexpr = eval_const_expressions(root, cexpr);
/// cexpr = (Node *) canonicalize_qual((Expr *) cexpr, true);
/// if (varno != 1) ChangeVarNodes(cexpr, 1, varno, 0);
/// result = list_concat(result, make_ands_implicit((Expr *) cexpr));
/// ```
///
/// The const-fold uses the root-less `eval_const_expressions_expr` variant —
/// faithful here because (per the C comment) CHECK constraints contain no
/// subqueries, so the only part of `eval_const_expressions` that needs `root`
/// (the sublink/Param machinery) cannot fire. Returns the implicit-AND clause
/// items interned into the planner (`root`) arena, with Vars stamped to `varno`.
fn process_check_constraint(
    root: &mut PlannerInfo,
    ccbin: &str,
    varno: i32,
) -> PgResult<Vec<NodeId>> {
    // The node-tree decode + transforms run in a transient context; the final
    // clause Exprs are cloned into the durable planner arena via `alloc_node`.
    let workcx = mcx::MemoryContext::new("process_check_constraint");
    let mcx = workcx.mcx();

    // cexpr = stringToNode(constr->check[i].ccbin);
    let node = backend_nodes_read_seams::string_to_node::call(mcx, ccbin)?;
    let cexpr = mcx::PgBox::into_inner(node)
        .into_expr()
        .ok_or_else(|| PgError::error("process_check_constraint: ccbin is not an Expr"))?;

    // cexpr = eval_const_expressions(root, cexpr);  (root-less variant: CHECK
    // constraints contain no subqueries, so the Param/sublink leg never runs.)
    let cexpr =
        backend_optimizer_plan_init_subselect_ext_seams::eval_const_expressions_expr::call(
            mcx, cexpr,
        )?;

    // cexpr = (Node *) canonicalize_qual((Expr *) cexpr, true);  (is_check = true)
    let canon =
        backend_optimizer_prep_prepqual_seams::canonicalize_qual::call(mcx, Some(cexpr), true)?;

    // make_ands_implicit((Expr *) cexpr): split the canonical boolean into an
    // implicit-AND list of independent clauses.
    let items = backend_nodes_core::makefuncs::make_ands_implicit(canon);

    // Intern each clause into the durable planner arena.
    let mut ids: Vec<NodeId> = Vec::with_capacity(items.len());
    for e in items {
        ids.push(root.alloc_node(e));
    }

    // if (varno != 1) ChangeVarNodes(cexpr, 1, varno, 0);  — restamp the Vars,
    // which `stringToNode` decoded with the catalog's varno == 1.
    if varno != 1 {
        plancat_ext::change_var_nodes::call(root, &ids, 1, varno);
    }

    Ok(ids)
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
    // The full bound algebra lives with partbounds; carry the consumer-layer
    // image the planner reads off `rel->boundinfo`: the scalars used by
    // `partitions_are_ordered` (strategy, default_index, interleaved_parts) plus
    // the bound algebra `partition_bounds_equal` compares for partitionwise join
    // (ndatums/nindexes/null_index, indexes[], and `'mcx`-free images of the
    // datums[][] / kind[][] matrices). A partitioned table with a partdesc
    // always has boundinfo.
    let boundinfo_carrier = partdesc.boundinfo.as_ref().map(|bi| {
        // interleaved_parts is the C `Bitmapset *` of interleaved LIST-partition
        // indexes; translate to the planner `Relids` representation (same member
        // numbering, distinct Bitmapset type) by walking its set bits.
        let interleaved: types_pathnodes::Relids = bi.interleaved_parts.as_ref().and_then(|b| {
            // Both representations are a flat `bitmapword[]` with identical
            // member numbering; copy the words directly (trim trailing zeros so
            // the empty set normalizes to None, matching C's NULL Bitmapset).
            let mut words = b.words.to_vec();
            while words.last() == Some(&0) {
                words.pop();
            }
            if words.is_empty() {
                None
            } else {
                Some(alloc::boxed::Box::new(types_pathnodes::Bitmapset { words }))
            }
        });
        // Convert one full-boundinfo Datum into the `'mcx`-free `DatumImage`
        // used for `datumIsEqual`-style comparison. Bound datums are plain
        // scalars or flat by-ref values; map by-value words verbatim and by-ref
        // / cstring payloads to their raw bytes.
        let datum_image = |d: &Datum<'_>| -> types_pathnodes::DatumImage {
            match d {
                Datum::ByVal(w) => types_pathnodes::DatumImage::ByVal(*w),
                Datum::ByRef(b) => types_pathnodes::DatumImage::Bytes(b.to_vec()),
                Datum::Cstring(s) => types_pathnodes::DatumImage::Bytes(s.clone().into_bytes()),
                // A partition bound never holds a composite/expanded/internal
                // datum; fall back to the zero word (datumIsEqual on it is only
                // reached if two bounds genuinely disagree elsewhere).
                _ => types_pathnodes::DatumImage::ByVal(0),
            }
        };
        let datums: Vec<Vec<types_pathnodes::DatumImage>> = bi
            .datums
            .iter()
            .map(|row| row.iter().map(&datum_image).collect())
            .collect();
        let kind: Option<Vec<Vec<i8>>> = bi.kind.as_ref().map(|k| {
            k.iter()
                .map(|row| row.iter().map(|rk| *rk as i8).collect())
                .collect()
        });
        Box::new(PartitionBoundInfoData {
            strategy: bi.strategy as i8,
            ndatums: bi.ndatums,
            nindexes: bi.nindexes,
            null_index: bi.null_index,
            default_index: bi.default_index,
            indexes: bi.indexes.to_vec(),
            datums,
            kind,
            interleaved_parts: interleaved,
        })
    });
    // rel->nparts = partdesc->nparts;
    let nparts = partdesc.nparts;

    {
        let r = root.rel_mut(rel);
        r.part_scheme = part_scheme;
        r.boundinfo = boundinfo_carrier;
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
