//! Port of `backend/utils/cache/partcache.c` — partition-key /
//! partition-constraint cache support routines.
//!
//! The *algorithm* lives here: the `RELKIND_PARTITIONED_TABLE` /
//! `relispartition` quick-exits, the lazy build of the partition key
//! (`RelationBuildPartitionKey`'s per-column loop: strategy validation,
//! opclass/support-function resolution, collation and type info), the
//! `get_partition_qual_relid` implicit-AND → bool-expr conversion, and the
//! parent-recursion of `generate_partition_qual` that builds and caches the
//! partition CHECK qual list.
//!
//! Caching is relcache-owned: C stores `rd_partkey`/`rd_partcheck` on the
//! relcache entry (under child contexts of `CacheMemoryContext`, preserved
//! across rebuilds). The owned-model handle [`types_rel::RelationData`] is a
//! read-only copy, so the cache get/set goes through the relcache owner's
//! seams (keyed by `rd_id`); the build logic between them is partcache's.
//! Every genuinely external operation — the `pg_partitioned_table` /
//! `pg_opclass` / `pg_class` syscache reads, `get_opfamily_proc`,
//! `format_type_be`, the fmgr lookup, `get_typlenbyvalalign`,
//! `get_partition_parent`, `get_rel_relispartition`, `exprType` family,
//! `makeBoolExpr`, `map_partition_varattnos`, `get_qual_from_partbound`,
//! `relation_open`/`close`, `check_stack_depth` — crosses its owner's seam and
//! panics until that owner lands.

#![allow(non_snake_case)]

use mcx::{slice_in, vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_core::primitive::{Oid, OidIsValid};
use types_error::{PgError, PgResult, ERRCODE_INVALID_OBJECT_DEFINITION};
use types_hash::HASHEXTENDED_PROC;
use types_nodes::nodes::Node;
use types_nodes::Expr;
use types_partition::{
    PartKeyTypeInfo, PartitionKeyData, PartitionStrategy, BTORDER_PROC,
    PARTITION_STRATEGY_HASH, PARTITION_STRATEGY_LIST, PARTITION_STRATEGY_RANGE,
};
use types_rel::{Relation, RelationData};
use types_storage::lock::{AccessShareLock, NoLock};
use types_tuple::access::RELKIND_PARTITIONED_TABLE;

use backend_access_common_relation_seams as relation_seam;
use backend_catalog_partition_seams as partition_seam;
use backend_nodes_makefuncs_seams as makefuncs_seam;
use backend_nodes_nodeFuncs_seams as nodefuncs_seam;
use backend_partitioning_partbounds_seams as partbounds_seam;
use backend_utils_adt_format_type_seams as format_type_seam;
use backend_utils_cache_lsyscache_seams as lsyscache_seam;
use backend_utils_cache_relcache_seams as relcache_seam;
use backend_utils_cache_syscache_seams as syscache_seam;
use backend_utils_fmgr_fmgr_seams as fmgr_seam;
use backend_utils_misc_stack_depth_seams as stack_depth_seam;

/// `AND_EXPR` (`nodes/primnodes.h`) — the `BoolExprType` for `makeBoolExpr`.
const AND_EXPR_LOCATION: i32 = -1;

/// `elog(ERROR, ...)` — an internal error with the default
/// `ERRCODE_INTERNAL_ERROR` SQLSTATE.
fn elog_error(msg: String) -> PgError {
    PgError::error(msg)
}

/*
 * RelationGetPartitionKey -- get partition key, if relation is partitioned
 *
 * Partition keys are not allowed to change after the partitioned rel is
 * created. RelationClearRelation preserves rd_partkey across relcache
 * rebuilds, as long as the relation is open, so the cached value is stable
 * for as long as the caller holds the relation open.
 */
pub fn RelationGetPartitionKey<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &RelationData<'_>,
) -> PgResult<Option<PartitionKeyData<'mcx>>> {
    // if (rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE) return NULL;
    if rel.rd_rel.relkind != RELKIND_PARTITIONED_TABLE {
        return Ok(None);
    }

    // if (unlikely(rel->rd_partkey == NULL)) RelationBuildPartitionKey(rel);
    if relcache_seam::relation_get_partkey::call(mcx, rel.rd_id)?.is_none() {
        RelationBuildPartitionKey(mcx, rel)?;
    }

    // return rel->rd_partkey;
    relcache_seam::relation_get_partkey::call(mcx, rel.rd_id)
}

/*
 * RelationBuildPartitionKey
 *		Build partition key data of relation, and attach to relcache
 *
 * Partitioning key data is a complex structure; C gives it its own memory
 * context (a child of CacheMemoryContext) so it can be freed wholesale on a
 * relcache flush. Here the built key is allocated in `mcx` and handed to the
 * relcache owner (`relation_set_partkey`), which copies it into the entry's
 * long-lived context; an error partway through leaks nothing because `mcx` is
 * the caller's transient context.
 */
fn RelationBuildPartitionKey<'mcx>(mcx: Mcx<'mcx>, relation: &RelationData<'_>) -> PgResult<()> {
    // tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(RelationGetRelid(relation)));
    // if (!HeapTupleIsValid(tuple)) elog(ERROR, "cache lookup failed for ...");
    let relid = relation.rd_id;
    let tuple = match syscache_seam::open_partrel_tuple::call(mcx, relid)? {
        Some(t) => t,
        None => {
            return Err(elog_error(format!(
                "cache lookup failed for partition key of relation {relid}"
            )))
        }
    };

    // form = GETSTRUCT(tuple);
    // key->strategy = form->partstrat; key->partnatts = form->partnatts;
    let strategy: PartitionStrategy = tuple.strategy;
    let partnatts: i16 = tuple.partnatts;

    // Validate partition strategy code.
    if strategy != PARTITION_STRATEGY_LIST
        && strategy != PARTITION_STRATEGY_RANGE
        && strategy != PARTITION_STRATEGY_HASH
    {
        // elog(ERROR, "invalid partition strategy \"%c\"", key->strategy);
        return Err(elog_error(format!(
            "invalid partition strategy \"{}\"",
            strategy as u8 as char
        )));
    }

    // attrs = form->partattrs.values;
    let attrs = &tuple.partattrs;
    // opclass = ... partclass; collation = ... partcollation;
    let opclass = &tuple.partclass;
    let collation = &tuple.partcollation;

    let npk = partnatts as usize;

    // Allocate the assorted per-attribute arrays (palloc0 of length partnatts).
    let mut partattrs = pg_zeroed::<i16>(mcx, npk)?;
    let mut partopfamily = pg_zeroed::<Oid>(mcx, npk)?;
    let mut partopcintype = pg_zeroed::<Oid>(mcx, npk)?;
    let mut partcollation = pg_zeroed::<Oid>(mcx, npk)?;
    let mut parttypid = pg_zeroed::<Oid>(mcx, npk)?;
    let mut parttypmod = pg_zeroed::<i32>(mcx, npk)?;
    let mut parttyplen = pg_zeroed::<i16>(mcx, npk)?;
    let mut parttypbyval = pg_zeroed::<bool>(mcx, npk)?;
    let mut parttypalign = pg_zeroed::<i8>(mcx, npk)?;
    let mut parttypcoll = pg_zeroed::<Oid>(mcx, npk)?;
    let mut partsupfunc: PgVec<types_core::fmgr::FmgrInfo> = vec_with_capacity_in(mcx, npk)?;

    // procnum = (strategy == HASH) ? HASHEXTENDED_PROC : BTORDER_PROC;
    let procnum: i16 = if strategy == PARTITION_STRATEGY_HASH {
        HASHEXTENDED_PROC as i16
    } else {
        BTORDER_PROC
    };

    // memcpy(key->partattrs, attrs, partnatts * sizeof(int16));
    partattrs[..npk].copy_from_slice(&attrs[..npk]);

    // partexprs_item = list_head(key->partexprs);
    let mut partexprs_idx: usize = 0;

    let mut i = 0usize;
    while i < npk {
        // AttrNumber attno = key->partattrs[i];
        let attno = partattrs[i];

        // Collect opfamily information.
        // opclasstup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass->values[i]));
        // if (!HeapTupleIsValid(opclasstup)) elog(ERROR, "cache lookup failed for opclass %u", ...);
        let opclassform = match syscache_seam::search_opclass::call(mcx, opclass[i])? {
            Some(f) => f,
            None => {
                return Err(elog_error(format!(
                    "cache lookup failed for opclass {}",
                    opclass[i]
                )))
            }
        };

        // key->partopfamily[i]  = opclassform->opcfamily;
        // key->partopcintype[i] = opclassform->opcintype;
        partopfamily[i] = opclassform.opcfamily;
        partopcintype[i] = opclassform.opcintype;

        // funcid = get_opfamily_proc(opcfamily, opcintype, opcintype, procnum);
        let funcid = lsyscache_seam::get_opfamily_proc::call(
            opclassform.opcfamily,
            opclassform.opcintype,
            opclassform.opcintype,
            procnum,
        )?;
        if !OidIsValid(funcid) {
            // ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
            //   errmsg("operator class \"%s\" of access method %s is missing
            //           support function %d for type %s", ...)));
            let amname = if strategy == PARTITION_STRATEGY_HASH {
                "hash"
            } else {
                "btree"
            };
            let typname = format_type_seam::format_type_be::call(mcx, opclassform.opcintype)?;
            return Err(PgError::error(format!(
                "operator class \"{}\" of access method {} is missing support function {} for type {}",
                opclassform.opcname.as_str(),
                amname,
                procnum,
                typname.as_str()
            ))
            .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION));
        }

        // fmgr_info_cxt(funcid, &key->partsupfunc[i], partkeycxt);
        // The owned FmgrInfo carries only the OID (re-resolved at call time);
        // preserve C's eager lookup-failure surface via fmgr_info_check.
        fmgr_seam::fmgr_info_check::call(funcid)?;
        partsupfunc.push(types_core::fmgr::FmgrInfo { fn_oid: funcid });

        // key->partcollation[i] = collation->values[i];
        partcollation[i] = collation[i];

        // Collect type information.
        let tinfo: PartKeyTypeInfo = if attno != 0 {
            // Form_pg_attribute att = TupleDescAttr(relation->rd_att, attno - 1);
            let att = relation.rd_att.attr((attno - 1) as usize);
            let typid = att.atttypid;
            let typmod = att.atttypmod;
            let typcoll = att.attcollation;
            // get_typlenbyvalalign(typid, &typlen, &typbyval, &typalign);
            let lba = lsyscache_seam::get_typlenbyvalalign::call(typid)?;
            PartKeyTypeInfo {
                typid,
                typmod,
                typcoll,
                typlen: lba.typlen,
                typbyval: lba.typbyval,
                typalign: lba.typalign,
            }
        } else {
            // if (partexprs_item == NULL) elog(ERROR, "wrong number of partition key expressions");
            if partexprs_idx >= tuple.partexprs.len() {
                return Err(elog_error(String::from(
                    "wrong number of partition key expressions",
                )));
            }
            // key->parttypid[i]   = exprType(lfirst(partexprs_item));
            // key->parttypmod[i]  = exprTypmod(lfirst(partexprs_item));
            // key->parttypcoll[i] = exprCollation(lfirst(partexprs_item));
            let expr: &Expr = &tuple.partexprs[partexprs_idx];
            let eti = nodefuncs_seam::expr_type_info::call(expr)?;
            // get_typlenbyvalalign(typid, ...);
            let lba = lsyscache_seam::get_typlenbyvalalign::call(eti.typid)?;
            // partexprs_item = lnext(key->partexprs, partexprs_item);
            partexprs_idx += 1;
            PartKeyTypeInfo {
                typid: eti.typid,
                typmod: eti.typmod,
                typcoll: eti.collation,
                typlen: lba.typlen,
                typbyval: lba.typbyval,
                typalign: lba.typalign,
            }
        };

        parttypid[i] = tinfo.typid;
        parttypmod[i] = tinfo.typmod;
        parttypcoll[i] = tinfo.typcoll;
        parttyplen[i] = tinfo.typlen;
        parttypbyval[i] = tinfo.typbyval;
        parttypalign[i] = tinfo.typalign;

        i += 1;
    }
    // ReleaseSysCache(opclasstup) / ReleaseSysCache(tuple) — subsumed by owning
    // the projected rows by value.

    // partexprs come from the (already-processed) PARTRELID tuple, copied here.
    let partexprs: PgVec<Expr> = slice_in(mcx, &tuple.partexprs)?;

    let key = PartitionKeyData {
        strategy,
        partnatts,
        partattrs,
        partexprs,
        partopfamily,
        partopcintype,
        partsupfunc,
        partcollation,
        parttypid,
        parttypmod,
        parttyplen,
        parttypbyval,
        parttypalign,
        parttypcoll,
    };

    // MemoryContextSetParent(partkeycxt, CacheMemoryContext);
    // relation->rd_partkeycxt = partkeycxt; relation->rd_partkey = key;
    relcache_seam::relation_set_partkey::call(relid, key)
}

/*
 * RelationGetPartitionQual
 *
 * Returns a list of partition quals.
 */
pub fn RelationGetPartitionQual<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &RelationData<'_>,
) -> PgResult<PgVec<'mcx, Node<'mcx>>> {
    // Quick exit: if (!rel->rd_rel->relispartition) return NIL;
    if !rel.rd_rel.relispartition {
        return Ok(PgVec::new_in(mcx));
    }

    generate_partition_qual(mcx, rel)
}

/*
 * get_partition_qual_relid
 *
 * Returns an expression tree describing the passed-in relation's partition
 * constraint. Returns NULL if the relation is not found, is not a partition,
 * or has no partition constraint (this supports a SQL function passed any OID).
 */
pub fn get_partition_qual_relid<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
) -> PgResult<Option<PgBox<'mcx, Node<'mcx>>>> {
    // Expr *result = NULL;
    let mut result: Option<PgBox<'mcx, Node<'mcx>>> = None;

    // Do the work only if this relation exists and is a partition.
    if lsyscache_seam::get_rel_relispartition::call(relid)? {
        // Relation rel = relation_open(relid, AccessShareLock);
        let rel: Relation<'mcx> = relation_seam::relation_open::call(mcx, relid, AccessShareLock)?;

        // and_args = generate_partition_qual(rel);
        let and_args = generate_partition_qual(mcx, &rel)?;

        // Convert implicit-AND list format to boolean expression.
        if and_args.is_empty() {
            // and_args == NIL → result = NULL;
            result = None;
        } else if and_args.len() > 1 {
            // result = makeBoolExpr(AND_EXPR, and_args, -1);
            result = Some(makefuncs_seam::make_and_boolexpr::call(
                mcx,
                and_args,
                AND_EXPR_LOCATION,
            )?);
        } else {
            // result = linitial(and_args); — the sole element (len == 1 here).
            let only = and_args.into_iter().next();
            result = match only {
                Some(node) => Some(mcx::alloc_in(mcx, node)?),
                None => None,
            };
        }

        // Keep the lock, to allow safe deparsing against the rel by caller.
        rel.close(NoLock)?;
    }

    Ok(result)
}

/*
 * generate_partition_qual
 *
 * Generate partition predicate from rel's partition bound expression. Returns
 * a NIL list (empty `PgVec`) if there is no predicate.
 *
 * We cache a copy of the result in the relcache entry; the working result is
 * built first and only written to the cache at the end, so a failure partway
 * through leaves nothing corrupt.
 */
fn generate_partition_qual<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &RelationData<'_>,
) -> PgResult<PgVec<'mcx, Node<'mcx>>> {
    // Guard against stack overflow due to overly deep partition tree.
    stack_depth_seam::check_stack_depth::call()?;

    // If we already cached the result, just return a copy.
    let (valid, cached) = relcache_seam::relation_get_partcheck::call(mcx, rel.rd_id)?;
    if valid {
        // return copyObject(rel->rd_partcheck);
        return Ok(cached);
    }

    // Grab at least an AccessShareLock on the parent table. Must do this even
    // if the partition has been partially detached.
    let parentrelid = partition_seam::get_partition_parent::call(rel.rd_id, true)?;
    let parent: Relation<'mcx> =
        relation_seam::relation_open::call(mcx, parentrelid, AccessShareLock)?;

    // Get pg_class.relpartbound, parse it, and build my_qual:
    //   tuple = SearchSysCache1(RELOID, ...); (→ "cache lookup failed for relation %u")
    //   boundDatum = SysCacheGetAttr(RELOID, ..., relpartbound, &isnull);
    //   if (!isnull) {
    //     bound = castNode(PartitionBoundSpec, stringToNode(TextDatumGetCString(boundDatum)));
    //     my_qual = get_qual_from_partbound(parent, bound);
    //   }
    let my_qual = partbounds_seam::qual_from_partbound::call(mcx, rel.rd_id, &parent)?;

    // Add the parent's quals to the list (if any).
    let result: PgVec<Node> = if parent.rd_rel.relispartition {
        // result = list_concat(generate_partition_qual(parent), my_qual);
        let mut parent_qual = generate_partition_qual(mcx, &parent)?;
        list_concat(mcx, &mut parent_qual, my_qual)?;
        parent_qual
    } else {
        my_qual
    };

    // Change Vars to have partition's attnos instead of the parent's. Done after
    // concatenating the parent's quals; it's safe to assume varno = 1.
    let result =
        partition_seam::map_partition_varattnos::call(mcx, result, 1, rel, &parent)?;

    // Save a copy in the relcache (rd_partcheck = copyObject(result); the
    // relcache owner copies into rd_partcheckcxt, then sets rd_partcheckvalid).
    let cache_copy = clone_node_list(mcx, &result)?;
    relcache_seam::relation_set_partcheck::call(rel.rd_id, cache_copy)?;

    // Keep the parent locked until commit.
    parent.close(NoLock)?;

    // Return the working copy to the caller.
    Ok(result)
}

/* --------------------------------------------------------------------------
 * Alloc helpers. Every growable allocation here is bounded by `partnatts`
 * (a small catalog-fixed key-column count) or a seam-supplied node-list
 * length; each uses the fallible `mcx` APIs so a hostile/huge bound surfaces
 * as an out-of-memory `PgError` rather than aborting.
 * ------------------------------------------------------------------------ */

/// `MemoryContextAllocZero(cxt, n * sizeof(T))` — a zero-initialized,
/// length-`n` array allocated in `mcx`.
fn pg_zeroed<'mcx, T: Copy + Default>(mcx: Mcx<'mcx>, n: usize) -> PgResult<PgVec<'mcx, T>> {
    let mut v = vec_with_capacity_in::<T>(mcx, n)?;
    for _ in 0..n {
        v.push(T::default());
    }
    Ok(v)
}

/// `list_concat(dst, src)` — append `src` onto `dst` (the C reuses dst's cells
/// and copies src's; here we move src's nodes in).
fn list_concat<'mcx>(
    mcx: Mcx<'mcx>,
    dst: &mut PgVec<'mcx, Node<'mcx>>,
    src: PgVec<'mcx, Node<'mcx>>,
) -> PgResult<()> {
    dst.try_reserve(src.len())
        .map_err(|_| mcx.oom(src.len().saturating_mul(core::mem::size_of::<Node>())))?;
    dst.extend(src);
    Ok(())
}

/// `copyObject(list)` over a node list — a fallibly-grown deep clone into
/// `mcx`.
fn clone_node_list<'mcx>(
    mcx: Mcx<'mcx>,
    src: &[Node<'_>],
) -> PgResult<PgVec<'mcx, Node<'mcx>>> {
    let mut out = vec_with_capacity_in::<Node>(mcx, src.len())?;
    for n in src {
        out.push(n.clone_in(mcx)?);
    }
    Ok(out)
}

/// Install every seam this crate owns. partcache itself declares no inward
/// seams yet (no ported caller crosses a cycle into it), so this is empty —
/// kept for the uniform `seams-init` shape.
pub fn init_seams() {}
