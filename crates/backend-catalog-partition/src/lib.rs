//! `src/backend/catalog/partition.c` (PostgreSQL 18.3) — partitioning-related
//! catalog data structures and functions:
//!   * [`get_partition_parent`] / `get_partition_parent_worker`
//!   * [`get_partition_ancestors`] / `get_partition_ancestors_worker`
//!   * [`index_get_partition`]
//!   * [`map_partition_varattnos`]
//!   * [`has_partition_attrs`]
//!   * [`get_default_partition_oid`] / [`update_default_partition_oid`]
//!   * [`get_proposed_default_constraint`]
//!
//! Signature mapping:
//! * C `List *` of OIDs (`get_partition_ancestors`) is `PgVec<'mcx, Oid>` in
//!   the caller's `mcx` (immediate parent first, topmost last — the C
//!   `lappend_oid` order; the empty hierarchy is the empty vec / C `NIL`).
//! * Node lists (`map_partition_varattnos`, `get_proposed_default_constraint`)
//!   are `PgVec<'mcx, Node<'mcx>>`; the empty list is the C `NIL`.
//! * The C `Bitmapset *attnums` of `has_partition_attrs` is
//!   `Option<&Bitmapset>` (`None` is the C `NULL`); the `bool *used_in_expr`
//!   out-param is `Option<&mut bool>` (`None` is the C `NULL`).
//! * `get_partition_parent` opens pg_inherits and returns only an `Oid`, so it
//!   runs its scan in a local short-lived context (C: `CurrentMemoryContext`).
//!
//! The pg_inherits index scan over `(inhrelid == relid, inhseqno == 1)` is
//! done in-crate against the opened catalog relation (the genam scan crosses
//! the access boundary via [`backend_access_index_genam_seams`]); the
//! attribute-map build + Var rewrite, the partition-key read, the syscache
//! reads, the catalog update, and the optimizer simplification cross to their
//! owning crates directly (acyclic) or through the owner seam where the catalog
//! write half is not yet ported.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::format;

use mcx::{vec_with_capacity_in, Mcx, MemoryContext, PgVec};

use types_catalog::pg_inherits::{
    Anum_pg_inherits_inhrelid, Anum_pg_inherits_inhseqno, FormData_pg_inherits,
    InheritsRelationId, InheritsRelidSeqnoIndexId, Natts_pg_inherits,
};
use types_core::fmgr::{F_INT4EQ, F_OIDEQ};
use types_core::primitive::{AttrNumber, InvalidOid, Oid, OidIsValid};
use types_error::{PgResult, ERROR};
use types_nodes::nodes::Node;
use types_nodes::primnodes::Expr;
use types_pathnodes::Bitmapset;
use types_rel::{Relation, RelationData};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::AccessShareLock;
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_access_common_heaptuple::heap_deform_tuple;
use backend_access_common_scankey::ScanKeyInit;
use backend_access_table_table as table;
use backend_utils_error::ereport;

use backend_access_common_next::attmap::build_attrmap_by_name;
use backend_nodes_core::makefuncs::{make_ands_explicit, make_ands_implicit, make_notclause};
use backend_optimizer_util_clauses::eval_const_expressions;
use backend_optimizer_util_vars::pull_varattnos;
use backend_optimizer_prep_prepqual::canonicalize_qual;
use backend_rewrite_core::map_variable_attnos;

use backend_access_index_genam_seams as genam_seams;
use backend_utils_cache_lsyscache_seams as lsyscache_seams;
use backend_utils_cache_partcache_seams as partcache_seams;
use backend_utils_cache_relcache_seams as relcache_seams;
use backend_utils_cache_syscache_seams as syscache_seams;

/// `FirstLowInvalidHeapAttributeNumber` (`access/sysattr.h`): the lowest system
/// attribute number minus one; the offset that turns a (possibly system)
/// attno into a non-negative bitmapset index in the `pull_varattnos` /
/// `has_partition_attrs` convention.
const FirstLowInvalidHeapAttributeNumber: i32 = -8;

/// `(Form_pg_inherits) GETSTRUCT(tup)` — interpret one deformed pg_inherits row.
/// Every pg_inherits column is fixed-width and NOT NULL.
fn form_pg_inherits(values: &[Datum<'_>]) -> FormData_pg_inherits {
    debug_assert_eq!(values.len(), Natts_pg_inherits);
    let col = |attno: AttrNumber| &values[attno as usize - 1];
    FormData_pg_inherits {
        inhrelid: col(Anum_pg_inherits_inhrelid).as_oid(),
        inhparent: col(2).as_oid(),
        inhseqno: col(Anum_pg_inherits_inhseqno).as_i32(),
        inhdetachpending: col(4).as_bool(),
    }
}

/*
 * get_partition_parent
 *		Obtain direct parent of given relation
 *
 * Returns inheritance parent of a partition by scanning pg_inherits
 *
 * If the partition is in the process of being detached, an error is thrown,
 * unless even_if_detached is passed as true.
 *
 * Note: Because this function assumes that the relation whose OID is passed
 * as an argument will have precisely one parent, it should only be called
 * when it is known that the relation is a partition.
 */
pub fn get_partition_parent(relid: Oid, even_if_detached: bool) -> PgResult<Oid> {
    // The C runs in CurrentMemoryContext; here the only escaping value is the
    // result Oid, so the scan runs in a local short-lived context that drops
    // when this function returns.
    let ctx = MemoryContext::new("get_partition_parent");
    let mcx = ctx.mcx();

    // catalogRelation = table_open(InheritsRelationId, AccessShareLock);
    let catalogRelation = table::table_open(mcx, InheritsRelationId, AccessShareLock)?;

    // result = get_partition_parent_worker(catalogRelation, relid, &detach_pending);
    let mut detach_pending = false;
    let result = get_partition_parent_worker(&catalogRelation, relid, &mut detach_pending)?;

    // if (!OidIsValid(result))
    //     elog(ERROR, "could not find tuple for parent of relation %u", relid);
    if !OidIsValid(result) {
        return Err(ereport(ERROR)
            .errmsg(format!("could not find tuple for parent of relation {relid}"))
            .into_error());
    }

    // if (detach_pending && !even_if_detached)
    //     elog(ERROR, "relation %u has no parent because it's being detached", relid);
    if detach_pending && !even_if_detached {
        return Err(ereport(ERROR)
            .errmsg(format!(
                "relation {relid} has no parent because it's being detached"
            ))
            .into_error());
    }

    // table_close(catalogRelation, AccessShareLock);
    catalogRelation.close(AccessShareLock)?;

    // return result;
    Ok(result)
}

/*
 * get_partition_parent_worker
 *		Scan the pg_inherits relation to return the OID of the parent of the
 *		given relation
 *
 * If the partition is being detached, *detach_pending is set true (but the
 * original parent is still returned.)
 */
fn get_partition_parent_worker(
    inhRel: &RelationData<'_>,
    relid: Oid,
    detach_pending: &mut bool,
) -> PgResult<Oid> {
    // Oid result = InvalidOid;
    let mut result: Oid = InvalidOid;

    // *detach_pending = false;
    *detach_pending = false;

    // ScanKeyInit(&key[0], Anum_pg_inherits_inhrelid, BTEqualStrategyNumber,
    //             F_OIDEQ, ObjectIdGetDatum(relid));
    let mut key0 = ScanKeyData::empty();
    ScanKeyInit(
        &mut key0,
        Anum_pg_inherits_inhrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(relid),
    )?;
    // ScanKeyInit(&key[1], Anum_pg_inherits_inhseqno, BTEqualStrategyNumber,
    //             F_INT4EQ, Int32GetDatum(1));
    let mut key1 = ScanKeyData::empty();
    ScanKeyInit(
        &mut key1,
        Anum_pg_inherits_inhseqno,
        BTEqualStrategyNumber,
        F_INT4EQ,
        Datum::from_i32(1),
    )?;
    let keys = [key0, key1];

    // scan = systable_beginscan(inhRel, InheritsRelidSeqnoIndexId, true, NULL, 2, key);
    let mut scan =
        genam_seams::systable_beginscan::call(inhRel, InheritsRelidSeqnoIndexId, true, None, &keys)?;

    // tuple = systable_getnext(scan);
    // if (HeapTupleIsValid(tuple))
    let row_ctx = MemoryContext::new("get_partition_parent_worker row");
    if let Some(tup) = genam_seams::systable_getnext::call(row_ctx.mcx(), scan.desc_mut())? {
        // Form_pg_inherits form = (Form_pg_inherits) GETSTRUCT(tuple);
        let cols = heap_deform_tuple(row_ctx.mcx(), &tup.tuple, &inhRel.rd_att, &tup.data)?;
        let mut values: PgVec<'_, Datum<'_>> = vec_with_capacity_in(row_ctx.mcx(), cols.len())?;
        for (value, _null) in cols.iter() {
            values.push(value.clone());
        }
        let form = form_pg_inherits(&values);

        // Let caller know of partition being detached
        // if (form->inhdetachpending) *detach_pending = true;
        if form.inhdetachpending {
            *detach_pending = true;
        }
        // result = form->inhparent;
        result = form.inhparent;
    }

    // systable_endscan(scan);
    scan.end()?;

    // return result;
    Ok(result)
}

/*
 * get_partition_ancestors
 *		Obtain ancestors of given relation
 *
 * Returns a list of ancestors of the given relation.  The list is ordered:
 * The first element is the immediate parent and the last one is the topmost
 * parent in the partition hierarchy.
 *
 * Note: Because this function assumes that the relation whose OID is passed
 * as an argument and each ancestor will have precisely one parent, it should
 * only be called when it is known that the relation is a partition.
 */
pub fn get_partition_ancestors<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<PgVec<'mcx, Oid>> {
    // List *result = NIL;
    let mut result: PgVec<'mcx, Oid> = PgVec::new_in(mcx);

    // inhRel = table_open(InheritsRelationId, AccessShareLock);
    let inhRel = table::table_open(mcx, InheritsRelationId, AccessShareLock)?;

    // get_partition_ancestors_worker(inhRel, relid, &result);
    get_partition_ancestors_worker(mcx, &inhRel, relid, &mut result)?;

    // table_close(inhRel, AccessShareLock);
    inhRel.close(AccessShareLock)?;

    // return result;
    Ok(result)
}

/*
 * get_partition_ancestors_worker
 *		recursive worker for get_partition_ancestors
 */
fn get_partition_ancestors_worker<'mcx>(
    mcx: Mcx<'mcx>,
    inhRel: &RelationData<'_>,
    relid: Oid,
    ancestors: &mut PgVec<'mcx, Oid>,
) -> PgResult<()> {
    // Recursion ends at the topmost level, ie., when there's no parent; also
    // when the partition is being detached.
    // parentOid = get_partition_parent_worker(inhRel, relid, &detach_pending);
    let mut detach_pending = false;
    let parentOid = get_partition_parent_worker(inhRel, relid, &mut detach_pending)?;

    // if (parentOid == InvalidOid || detach_pending) return;
    if parentOid == InvalidOid || detach_pending {
        return Ok(());
    }

    // *ancestors = lappend_oid(*ancestors, parentOid);
    ancestors
        .try_reserve(1)
        .map_err(|_| mcx.oom(core::mem::size_of::<Oid>()))?;
    ancestors.push(parentOid);
    // get_partition_ancestors_worker(inhRel, parentOid, ancestors);
    get_partition_ancestors_worker(mcx, inhRel, parentOid, ancestors)
}

/*
 * index_get_partition
 *		Return the OID of index of the given partition that is a child
 *		of the given index, or InvalidOid if there isn't one.
 */
pub fn index_get_partition(partition: &Relation<'_>, indexId: Oid) -> PgResult<Oid> {
    let ctx = MemoryContext::new("index_get_partition");
    let mcx = ctx.mcx();

    // List *idxlist = RelationGetIndexList(partition);
    let idxlist = relcache_seams::relation_get_index_list::call(mcx, partition)?;

    // foreach(l, idxlist)
    for partIdx in idxlist.iter().copied() {
        // tup = SearchSysCache1(RELOID, ObjectIdGetDatum(partIdx));
        // if (!HeapTupleIsValid(tup))
        //     elog(ERROR, "cache lookup failed for relation %u", partIdx);
        // classForm = (Form_pg_class) GETSTRUCT(tup);
        // ispartition = classForm->relispartition;
        // ReleaseSysCache(tup);
        let ispartition = match syscache_seams::rel_relispartition::call(partIdx)? {
            Some(b) => b,
            None => {
                return Err(ereport(ERROR)
                    .errmsg(format!("cache lookup failed for relation {partIdx}"))
                    .into_error());
            }
        };

        // if (!ispartition) continue;
        if !ispartition {
            continue;
        }

        // if (get_partition_parent(partIdx, false) == indexId) { list_free(idxlist); return partIdx; }
        if get_partition_parent(partIdx, false)? == indexId {
            return Ok(partIdx);
        }
    }

    // list_free(idxlist); return InvalidOid;
    Ok(InvalidOid)
}

/*
 * map_partition_varattnos - maps varattnos of all Vars in 'expr' (that have
 * varno 'fromrel_varno') from the attnums of 'from_rel' to the attnums of
 * 'to_rel', each of which may be either a leaf partition or a partitioned
 * table, but both of which must be from the same partitioning hierarchy.
 *
 * We need this because even though all of the same column names must be
 * present in all relations in the hierarchy, and they must also have the
 * same types, the attnums may be different.
 *
 * Note: this will work on any node tree, so really the argument and result
 * should be declared "Node *".  But a substantial majority of the callers
 * are working on Lists, so it's less messy to do the casts internally.
 */
pub fn map_partition_varattnos<'mcx, 'r>(
    mcx: Mcx<'mcx>,
    mut exprs: PgVec<'mcx, Node<'mcx>>,
    fromrel_varno: i32,
    to_rel: &RelationData<'r>,
    from_rel: &RelationData<'r>,
) -> PgResult<PgVec<'mcx, Node<'mcx>>> {
    // if (expr != NIL)
    if !exprs.is_empty() {
        // part_attmap = build_attrmap_by_name(RelationGetDescr(to_rel),
        //                                     RelationGetDescr(from_rel), false);
        let part_attmap = build_attrmap_by_name(mcx, &to_rel.rd_att, &from_rel.rd_att, false)?;

        // expr = (List *) map_variable_attnos((Node *) expr, fromrel_varno, 0,
        //                                     part_attmap,
        //                                     RelationGetForm(to_rel)->reltype,
        //                                     &found_whole_row);
        // Since we provided a to_rowtype, we may ignore found_whole_row.
        // The trimmed relcache descriptor (types-rel) drops `reltype`, so read
        // the relation's composite type OID via the immutable RELOID syscache
        // lookup (`get_rel_type_id`), equivalent to RelationGetForm(...)->reltype.
        let to_rowtype = lsyscache_seams::get_rel_type_id::call(to_rel.rd_id)?;
        for node in exprs.iter_mut() {
            let mut found_whole_row = false;
            map_variable_attnos(
                node,
                fromrel_varno,
                0,
                &part_attmap.attnums,
                to_rowtype,
                &mut found_whole_row,
            )?;
        }
    }

    // return expr;
    Ok(exprs)
}

/*
 * Checks if any of the 'attnums' is a partition key attribute for rel
 *
 * Sets *used_in_expr if any of the 'attnums' is found to be referenced in some
 * partition key expression.  It's possible for a column to be both used
 * directly and as part of an expression; if that happens, *used_in_expr may
 * end up as either true or false.  That's OK for current uses of this
 * function, because *used_in_expr is only used to tailor the error message
 * text.
 *
 * The C `Bitmapset *attnums` is `Option<&Bitmapset>` (`None` is the C `NULL`);
 * the `bool *used_in_expr` out-param is `Option<&mut bool>`. `rel` is the
 * caller's open relation (C: a `Relation` it already holds); the partition key
 * is read via `RelationGetPartitionKey` (partcache).
 */
pub fn has_partition_attrs<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    attnums: Option<&Bitmapset>,
    mut used_in_expr: Option<&mut bool>,
) -> PgResult<bool> {
    // if (attnums == NULL || rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
    //     return false;
    let attnums = match attnums {
        None => return Ok(false),
        Some(a) => a,
    };

    // key = RelationGetPartitionKey(rel);
    // A non-partitioned relation has no key (C: rd_rel->relkind check); the
    // partcache builder returns None for it, matching the relkind guard.
    let key = match partcache_seams::relation_get_partition_key::call(mcx, rel.alias())? {
        Some(key) => key,
        None => return Ok(false),
    };

    // partnatts = get_partition_natts(key);
    let partnatts = key.partnatts as usize;
    // partexprs_item = list_head(partexprs);
    let mut partexprs_item: usize = 0;

    // for (i = 0; i < partnatts; i++)
    for i in 0..partnatts {
        // AttrNumber partattno = get_partition_col_attnum(key, i);
        let partattno: AttrNumber = key.partattrs[i];

        // if (partattno != 0)
        if partattno != 0 {
            // if (bms_is_member(partattno - FirstLowInvalidHeapAttributeNumber, attnums))
            if bms_is_member(partattno as i32 - FirstLowInvalidHeapAttributeNumber, Some(attnums)) {
                // if (used_in_expr) *used_in_expr = false;
                if let Some(uie) = used_in_expr.as_deref_mut() {
                    *uie = false;
                }
                // return true;
                return Ok(true);
            }
        } else {
            // Arbitrary expression
            // Node *expr = (Node *) lfirst(partexprs_item);
            let expr = &key.partexprs[partexprs_item];
            // Bitmapset *expr_attrs = NULL;
            // Find all attributes referenced
            // pull_varattnos(expr, 1, &expr_attrs);
            let expr_node = Node::Expr(expr.clone());
            let expr_attrs = pull_varattnos(&expr_node, 1, None);
            // partexprs_item = lnext(partexprs, partexprs_item);
            partexprs_item += 1;

            // if (bms_overlap(attnums, expr_attrs))
            if bms_overlap(Some(attnums), expr_attrs.as_deref()) {
                // if (used_in_expr) *used_in_expr = true;
                if let Some(uie) = used_in_expr.as_deref_mut() {
                    *uie = true;
                }
                // return true;
                return Ok(true);
            }
        }
    }

    // return false;
    Ok(false)
}

/// `BITS_PER_BITMAPWORD` (nodes/bitmapset.h): 64 on LP64.
const BITS_PER_BITMAPWORD: i32 = 64;

/// `bms_is_member(x, a)` (nodes/bitmapset.c) over the `Bitmapset { words:
/// Vec<u64> }` word storage `pull_varattnos` produces (`types_pathnodes`,
/// distinct from the `types_nodes` planner-relids set the nodes-core ops use).
fn bms_is_member(x: i32, a: Option<&Bitmapset>) -> bool {
    if x < 0 {
        panic!("negative bitmapset member not allowed");
    }
    let a = match a {
        None => return false,
        Some(a) => a,
    };
    let wnum = (x / BITS_PER_BITMAPWORD) as usize;
    if wnum >= a.words.len() {
        return false;
    }
    a.words[wnum] & (1u64 << (x % BITS_PER_BITMAPWORD)) != 0
}

/// `bms_overlap(a, b)` (nodes/bitmapset.c) over the same word storage.
fn bms_overlap(a: Option<&Bitmapset>, b: Option<&Bitmapset>) -> bool {
    let (a, b) = match (a, b) {
        (Some(a), Some(b)) => (a, b),
        _ => return false,
    };
    let shortlen = a.words.len().min(b.words.len());
    for i in 0..shortlen {
        if a.words[i] & b.words[i] != 0 {
            return true;
        }
    }
    false
}

/*
 * get_default_partition_oid
 *
 * Given a relation OID, return the OID of the default partition, if one
 * exists.  Use get_default_oid_from_partdesc where possible, for
 * efficiency.
 */
pub fn get_default_partition_oid(parentId: Oid) -> PgResult<Oid> {
    // HeapTuple tuple;
    // Oid defaultPartId = InvalidOid;
    let mut defaultPartId: Oid = InvalidOid;

    // tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(parentId));
    // if (HeapTupleIsValid(tuple)) {
    //     part_table_form = (Form_pg_partitioned_table) GETSTRUCT(tuple);
    //     defaultPartId = part_table_form->partdefid;
    //     ReleaseSysCache(tuple);
    // }
    if let Some(partdefid) = syscache_seams::search_partrelid_partdefid::call(parentId)? {
        defaultPartId = partdefid;
    }

    // return defaultPartId;
    Ok(defaultPartId)
}

/*
 * update_default_partition_oid
 *
 * Update pg_partitioned_table.partdefid with a new default partition OID.
 */
pub fn update_default_partition_oid(parentId: Oid, defaultPartId: Oid) -> PgResult<()> {
    // pg_partitioned_table = table_open(PartitionedRelationId, RowExclusiveLock);
    // tuple = SearchSysCacheCopy1(PARTRELID, ObjectIdGetDatum(parentId));
    // if (!HeapTupleIsValid(tuple))
    //     elog(ERROR, "cache lookup failed for partition key of relation %u", parentId);
    // part_table_form = (Form_pg_partitioned_table) GETSTRUCT(tuple);
    // part_table_form->partdefid = defaultPartId;
    // CatalogTupleUpdate(pg_partitioned_table, &tuple->t_self, tuple);
    // heap_freetuple(tuple);
    // table_close(pg_partitioned_table, RowExclusiveLock);
    //
    // The whole open / syscache-copy / in-place partdefid write / form-and-
    // CatalogTupleUpdate / freetuple / close sequence rides on the heap-tuple
    // value layer and the RowExclusiveLock lifetime — it is the catalog-write
    // half (catalog/indexing.c owns CatalogTupleUpdate), which performs the
    // cache-lookup-failure elog itself.
    syscache_seams::update_default_partition_oid::call(parentId, defaultPartId)
}

/*
 * get_proposed_default_constraint
 *
 * This function returns the negation of new_part_constraints, which
 * would be an integral part of the default partition constraints after
 * addition of the partition to which the new_part_constraints belongs.
 */
pub fn get_proposed_default_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    new_part_constraints: PgVec<'mcx, Node<'mcx>>,
) -> PgResult<PgVec<'mcx, Node<'mcx>>> {
    // defPartConstraint = make_ands_explicit(new_part_constraints);
    let mut and_clauses: alloc::vec::Vec<Expr> =
        alloc::vec::Vec::with_capacity(new_part_constraints.len());
    for node in new_part_constraints {
        and_clauses.push(node_to_expr(node)?);
    }
    let defPartConstraint = make_ands_explicit(and_clauses);

    // Derive the partition constraints of default partition by negating the
    // given partition constraints. The partition constraint never evaluates
    // to NULL, so negating it like this is safe.
    // defPartConstraint = makeBoolExpr(NOT_EXPR, list_make1(defPartConstraint), -1);
    let defPartConstraint = make_notclause(defPartConstraint);

    // Simplify, to put the negated expression into canonical form
    // defPartConstraint = (Expr *) eval_const_expressions(NULL, (Node *) defPartConstraint);
    let defPartConstraint = eval_const_expressions(mcx, defPartConstraint)?;

    // defPartConstraint = canonicalize_qual(defPartConstraint, true);
    let defPartConstraint = canonicalize_qual(Some(defPartConstraint), true)?;

    // return make_ands_implicit(defPartConstraint);
    let implicit = make_ands_implicit(defPartConstraint);
    let mut result: PgVec<'mcx, Node<'mcx>> = vec_with_capacity_in(mcx, implicit.len())?;
    for e in implicit {
        result.push(Node::Expr(e));
    }
    Ok(result)
}

/// Unwrap an expression node into the `Expr` the makefuncs/optimizer layer
/// works on (the constraint list is always primitive expression nodes; C
/// stores them as `Node *` and casts).
fn node_to_expr(node: Node<'_>) -> PgResult<Expr> {
    let tag = node.tag();
    match node.into_expr() {
        Some(e) => Ok(e),
        None => Err(ereport(ERROR)
            .errmsg(format!(
                "unexpected non-expression node {:?} in partition constraint",
                tag
            ))
            .into_error()),
    }
}

/// Wire every seam this crate owns (`backend-catalog-partition-seams`) to its
/// real implementation. Called once from `seams-init::init_all()`.
pub fn init_seams() {
    backend_catalog_partition_seams::get_partition_parent::set(get_partition_parent);
    backend_catalog_partition_seams::get_partition_ancestors::set(get_partition_ancestors);
    backend_catalog_partition_seams::map_partition_varattnos::set(map_partition_varattnos);
}
