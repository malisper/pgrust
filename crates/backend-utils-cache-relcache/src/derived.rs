//! derived family — the per-relation derived caches built over the real store
//! (OWN logic).
//!
//! The derived-list builders (`RelationGetFKeyList`/`IndexList`/`StatExtList`/
//! `PrimaryKeyIndex`/`ReplicaIndex`/`IndexExpressions`/`IndexPredicate`/
//! `IndexAttrBitmap`/`IdentityKeyBitmap`/`ExclusionInfo`,
//! `RelationBuildPublicationDesc`, `RelationBuildRuleLock`) are relcache's OWN
//! logic over the real entry's `rd_indexlist`/`rd_*attr`/… fields, ported in
//! full here.
//!
//! Only the *genuine cross-unit* primitives are routed through owner seams
//! (panic until the owner lands): the catalog scans (`systable_beginscan`/
//! `getnext` over `pg_index`/`pg_constraint`/`pg_statistic_ext` — genam owner),
//! `index_open`/`index_close` (relation/indexam owner), and the node/rewrite/
//! publication vocabulary (`stringToNode`/`eval_const_expressions`/
//! `pull_varattnos`/`get_opcode`/… — node + rewrite owners). Those are the
//! `*_seam` helpers at the bottom of this module; the orchestration around them
//! is real and operates on the owned [`RelationData`] store.

use backend_access_index_genam_seams as genam_seam;
use backend_access_index_indexam_seams as indexam_seam;
use backend_nodes_read_seams as read_seam;
use backend_rewrite_rewriteDefine_seams::{
    set_rule_check_as_user as set_rule_check_as_user_seam,
    set_rule_check_as_user_node as set_rule_check_as_user_node_seam,
};
use backend_utils_cache_relcache_nodexform_seams as nodexform_seam;
use backend_utils_error::{ereport, PgResult};
use mcx::{Mcx, MemoryContext};
use types_rel::Relation;
use types_tuple::Datum;
use types_core::primitive::{AttrNumber, Oid, RegProcedure};
use types_core::{InvalidOid, OidIsValid};
use types_error::ERROR;
use types_tuple::{
    FirstLowInvalidHeapAttributeNumber, RELKIND_PARTITIONED_TABLE, REPLICA_IDENTITY_DEFAULT,
    REPLICA_IDENTITY_INDEX,
};

use crate::core_entry_store::entry::{
    FormPgIndex, RelationData, RewriteRule, RowSecurityDesc, RowSecurityPolicy, RuleLock,
};
use crate::core_entry_store::{with_rel, with_rel_mut};

/// `IndexAttrBitmapKind` (relcache.h) — which attribute-bitmap to fetch.
pub use types_relcache_entry::IndexAttrBitmapKind;

/* ==========================================================================
 * RelationGetFKeyList -- foreign-key cache-info list (rd_fkeylist).
 *
 * The orchestration (quick-exit on rd_fkeyvalid, pg_constraint scan, install
 * into the entry) is own logic. The per-tuple payload — the
 * `ForeignKeyCacheInfo` node and `DeconstructFkConstraintRow` (FK node
 * vocabulary, owned elsewhere) — is the genuine cross-unit piece, routed
 * through the scan seam: `scan_pg_constraint_fkeys_seam` returns the built
 * cache-info rows for `conrelid = relid`, the FK-node assembly performed by its
 * owner. We store them on the entry (presence flag here) and are done.
 * ======================================================================== */

/// `RelationGetFKeyList(relation)` (relcache.c): the relation's foreign-key
/// cache-info list, built from `pg_constraint` and cached in `rd_fkeylist`.
/// Returns the assembled `ForeignKeyCacheInfo` rows (the C return value the
/// planner's `get_relation_foreign_keys` walks); the presence flag is cached on
/// the entry. The owned model re-scans rather than reading a cached `rd_fkeylist`
/// payload (the entry caches only the presence flag), which is behaviorally the
/// same list — pg_constraint is unchanged within the planning snapshot.
pub fn RelationGetFKeyList(relation: Oid) -> PgResult<Vec<ForeignKeyCacheInfo>> {
    /*
     * Scan pg_constraint for entries having conrelid = this rel, keeping only
     * the foreign keys. The FK-node build (`ForeignKeyCacheInfo` +
     * `DeconstructFkConstraintRow`) is FK node vocabulary owned cross-unit; the
     * seam returns the assembled rows. The orchestration here mirrors C.
     */
    let relid = with_rel(relation, |rd| rd.rd_id);
    let fkeys = scan_pg_constraint_fkeys_seam(relid)?;

    /* Now mark the completed list saved in the relcache entry. */
    with_rel_mut(relation, |rd| rd.rd_fkeyvalid = true);
    Ok(fkeys)
}

/* ==========================================================================
 * RelationGetIndexList -- OIDs of indexes on this relation (rd_indexlist).
 * Full own logic over the store; only the pg_index scan is seamed.
 * ======================================================================== */

/// `RelationGetIndexList(relation)` (relcache.c): the OIDs of the relation's
/// indexes, built from `pg_index` and cached in `rd_indexlist` (+ `rd_pkindex`/
/// `rd_replidindex`). **Own logic.**
pub fn RelationGetIndexList(relation: Oid) -> PgResult<Vec<Oid>> {
    /* Quick exit if we already computed the list. */
    let (indexvalid, indexlist, replident, relkind, relid) = with_rel(relation, |rd| {
        (
            rd.rd_indexvalid,
            rd.rd_indexlist.clone(),
            rd.rd_rel.relreplident,
            rd.rd_rel.relkind,
            rd.rd_id,
        )
    });
    if indexvalid {
        return Ok(indexlist);
    }

    /*
     * We build the list we intend to return while doing the scan. The pg_index
     * scan is the genuine cross-unit primitive (genam owner); it yields, per
     * live row, the `FormPgIndex` form plus whether `indpred` is null. The
     * derivation below is own logic.
     */
    let mut result: Vec<Oid> = Vec::new();
    let mut pkey_index: Oid = InvalidOid;
    let mut candidate_index: Oid = InvalidOid;
    let mut pkdeferrable = false;

    let rows = scan_pg_index_seam(relid)?;
    for ScannedIndex {
        index,
        indpred_isnull,
    } in &rows
    {
        /*
         * Ignore any indexes that are currently being dropped. This will
         * prevent them from being searched, inserted into, or considered in
         * HOT-safety decisions.
         */
        if !index.indislive {
            continue;
        }

        /* add index's OID to result list */
        result.push(index.indexrelid);

        /*
         * Non-unique or predicate indexes aren't interesting for either oid
         * indexes or replication identity indexes, so don't check them.
         * Deferred ones are not useful for replication identity either; but we
         * do include them if they are PKs.
         */
        if !index.indisunique || !indpred_isnull {
            continue;
        }

        /*
         * Remember primary key index, if any. For regular tables we do this
         * only if the index is valid; but for partitioned tables, then we do it
         * even if it's invalid.
         */
        if index.indisprimary
            && (index.indisvalid || relkind == RELKIND_PARTITIONED_TABLE as i8)
        {
            pkey_index = index.indexrelid;
            pkdeferrable = !index.indimmediate;
        }

        if !index.indimmediate {
            continue;
        }

        if !index.indisvalid {
            continue;
        }

        /* remember explicitly chosen replica index */
        if index.indisreplident {
            candidate_index = index.indexrelid;
        }
    }

    /* Sort the result list into OID order, per API spec. */
    result.sort_unstable();

    /* Now save a copy of the completed list in the relcache entry. */
    with_rel_mut(relation, |rd| {
        rd.rd_indexlist = result.clone();
        rd.rd_pkindex = pkey_index;
        rd.rd_ispkdeferrable = pkdeferrable;
        if replident == REPLICA_IDENTITY_DEFAULT as i8 && OidIsValid(pkey_index) && !pkdeferrable {
            rd.rd_replidindex = pkey_index;
        } else if replident == REPLICA_IDENTITY_INDEX as i8 && OidIsValid(candidate_index) {
            rd.rd_replidindex = candidate_index;
        } else {
            rd.rd_replidindex = InvalidOid;
        }
        rd.rd_indexvalid = true;
    });

    Ok(result)
}

/* ==========================================================================
 * RelationGetStatExtList -- OIDs of extended-statistics objects (rd_statlist).
 * Full own logic; only the pg_statistic_ext scan is seamed.
 * ======================================================================== */

/// `RelationGetStatExtList(relation)` (relcache.c): the OIDs of the relation's
/// extended-statistics objects, cached in `rd_statlist`. **Own logic.**
pub fn RelationGetStatExtList(relation: Oid) -> PgResult<Vec<Oid>> {
    /* Quick exit if we already computed the list. */
    let (statvalid, statlist, relid) =
        with_rel(relation, |rd| (rd.rd_statvalid, rd.rd_statlist.clone(), rd.rd_id));
    if statvalid {
        return Ok(statlist);
    }

    /*
     * Scan pg_statistic_ext for entries having stxrelid = this rel (genam
     * owner). The seam returns the matching statistics-object OIDs.
     */
    let mut result = scan_pg_statistic_ext_seam(relid)?;

    /* Sort the result list into OID order, per API spec. */
    result.sort_unstable();

    /* Now save a copy of the completed list in the relcache entry. */
    with_rel_mut(relation, |rd| {
        rd.rd_statlist = result.clone();
        rd.rd_statvalid = true;
    });

    Ok(result)
}

/* ==========================================================================
 * RelationGetPrimaryKeyIndex / RelationGetReplicaIndex -- own logic over the
 * index list (force RelationGetIndexList first, then read the cached field).
 * ======================================================================== */

/// `RelationGetPrimaryKeyIndex(relation, deferrable_ok)` (relcache.c): the
/// primary-key index OID (forces `RelationGetIndexList` first).
pub fn RelationGetPrimaryKeyIndex(relation: Oid, deferrable_ok: bool) -> PgResult<Oid> {
    if !with_rel(relation, |rd| rd.rd_indexvalid) {
        /* RelationGetIndexList does the heavy lifting. */
        let _ilist = RelationGetIndexList(relation)?;
        debug_assert!(with_rel(relation, |rd| rd.rd_indexvalid));
    }

    Ok(with_rel(relation, |rd| {
        if deferrable_ok {
            rd.rd_pkindex
        } else if rd.rd_ispkdeferrable {
            InvalidOid
        } else {
            rd.rd_pkindex
        }
    }))
}

/// `RelationGetReplicaIndex(relation)` (relcache.c): the replica-identity
/// index OID.
pub fn RelationGetReplicaIndex(relation: Oid) -> PgResult<Oid> {
    if !with_rel(relation, |rd| rd.rd_indexvalid) {
        /* RelationGetIndexList does the heavy lifting. */
        let _ilist = RelationGetIndexList(relation)?;
        debug_assert!(with_rel(relation, |rd| rd.rd_indexvalid));
    }

    Ok(with_rel(relation, |rd| rd.rd_replidindex))
}

/* ==========================================================================
 * RelationGetIndexExpressions / RelationGetIndexPredicate -- node-tree caches.
 *
 * The node-tree transform (`stringToNode`/`eval_const_expressions`/
 * `canonicalize_qual`/`make_ands_implicit`/`fix_opfuncids`) is node vocabulary
 * owned cross-unit; routed through the seam. The own caching contract (build in
 * the caller's context, copy into the entry's index context) lives here.
 * ======================================================================== */

/// `RelationGetIndexExpressions(relation)` (relcache.c): the index's expression
/// trees (node vocabulary — seamed for the tree, own caching).
pub fn RelationGetIndexExpressions(relation: Oid) -> PgResult<()> {
    // Quick exit if there is nothing to do (relcache.c:5108-5111): the C tests
    // `rd_indextuple == NULL || heap_attisnull(rd_indextuple,
    // Anum_pg_index_indexprs)` and returns `NIL`. An index carries expression
    // columns iff some `indkey[i] == InvalidAttrNumber` (a zero key column is
    // the on-disk marker for an expression column, and `pg_index.indexprs` is
    // non-NULL exactly when such a column exists). The owned entry carries the
    // full `indkey` vector, so this faithful proxy is computable here without
    // touching the node-tree decode: a non-index relation (`rd_index == None`,
    // the C `rd_indextuple == NULL`) or an index whose `indkey` has no zero
    // entry returns `Ok(())` (== NIL). This is the path every system-catalog
    // index (all simple-column) takes.
    let has_expression_col = with_rel(relation, |rd| match &rd.rd_index {
        None => false,
        Some(idx) => idx
            .indkey
            .iter()
            .any(|&k| k == types_core::primitive::InvalidAttrNumber),
    });
    if !has_expression_col {
        return Ok(());
    }

    // An expression column IS present: the `indexprs` node-tree decode
    // (`stringToNode`/`eval_const_expressions`/`fix_opfuncids`) is node
    // vocabulary owned cross-unit and still unported — route through the
    // node-tree owner seam (mirror-PG-and-panic until `stringToNode` lands).
    index_expressions_seam(relation)
}

/// `RelationGetIndexPredicate(relation)` (relcache.c): the index's partial
/// predicate tree (node vocabulary — seamed for the tree, own caching).
pub fn RelationGetIndexPredicate(relation: Oid) -> PgResult<()> {
    index_predicate_seam(relation)
}

/// `RelationGetDummyIndexExpressions(relation)` (relcache.c): a list of dummy
/// `Const` nodes with the same types/typmods/collations as the index's real
/// expressions — used where we must not run user-defined code (ANALYZE,
/// planner). Returns `NIL` when the index has no expressions.
///
/// The entire body is node vocabulary: the quick-exit reads the raw
/// `rd_indextuple` `indexprs` datum (`heap_attisnull`), and the result is a
/// `List*` of `Const` nodes built via `stringToNode`/`makeConst` over
/// `exprType`/`exprTypmod`/`exprCollation` of the raw expression sub-trees.
/// None of `rd_indextuple` (the raw pg_index `HeapTuple`), the node list, nor
/// the `Const` constructors are representable on the owned entry, so the whole
/// routine routes through the node-tree owner seam. The presence-only quick
/// exit (no `rd_index`, i.e. not an index, or no expression columns) is the
/// own-logic shell. **Own shell + node-owner seam.**
pub fn RelationGetDummyIndexExpressions(relation: Oid) -> PgResult<()> {
    // Quick exit if there is nothing to do: the C tests `rd_indextuple == NULL
    // || heap_attisnull(rd_indextuple, Anum_pg_index_indexprs)`. In the owned
    // mirror, a non-index entry has no `rd_index` form at all (the C
    // `rd_indextuple == NULL` case). Whether the index actually carries
    // expression columns (`indexprs` not null) is only observable from the raw
    // index tuple's `indexprs` attribute, which the owned model does not carry;
    // that no-expressions short-circuit therefore lives behind the seam.
    if with_rel(relation, |rd| rd.rd_index.is_none()) {
        return Ok(());
    }

    // Extract raw node tree(s) from the index tuple, build the dummy Const list
    // (makeConst over exprType/exprTypmod/exprCollation of each raw sub-tree).
    // All node vocabulary; route through the node-tree owner seam.
    dummy_index_expressions_seam(relation)
}

/* ==========================================================================
 * RelationGetIndexAttrBitmap -- attribute bitmaps per index (rd_*attr).
 *
 * Own orchestration: the cached quick-exit, the restart-on-flush protocol, the
 * per-index attribute collection, and the store-into-entry are all own logic
 * over the store. The genuine cross-unit primitives are `index_open`/
 * `index_close` (relation owner) and the per-index expression/predicate node
 * pulls (`pull_varattnos` over `stringToNode` — node owner); they are routed
 * through `open_index_attrs_seam`, which returns the index's attribute
 * contributions (the collected offset members) for one index OID. The bitmap
 * merging across indexes is own logic here.
 * ======================================================================== */

/// `RelationGetIndexAttrBitmap(relation, attrKind)` (relcache.c): the requested
/// attribute bitmap, built (and cached on the entry) from the index list.
/// Returns the offset members. **Own logic.**
pub fn RelationGetIndexAttrBitmap(
    relation: Oid,
    attrKind: IndexAttrBitmapKind,
) -> PgResult<Vec<i32>> {
    /* Quick exit if we already computed the result. */
    if let Some(cached) = with_rel(relation, |rd| {
        if rd.rd_attrsvalid {
            Some(match attrKind {
                IndexAttrBitmapKind::Keys => rd.rd_keyattr.clone(),
                IndexAttrBitmapKind::PrimaryKey => rd.rd_pkattr.clone(),
                IndexAttrBitmapKind::Identity => rd.rd_idattr.clone(),
                IndexAttrBitmapKind::HotBlocking => rd.rd_hotblockingattr.clone(),
                IndexAttrBitmapKind::Summarized => rd.rd_summarizedattr.clone(),
            })
        } else {
            None
        }
    }) {
        return Ok(cached);
    }

    /* Fast path if definitely no indexes */
    if !with_rel(relation, |rd| rd.rd_rel.relhasindex) {
        return Ok(Vec::new());
    }

    /*
     * Get cached list of index OIDs. If we have to start over, we do so here.
     */
    let (uindexattrs, pkindexattrs, idindexattrs, hotblockingattrs, summarizedattrs) = loop {
        let indexoidlist = RelationGetIndexList(relation)?;

        /* Fall out if no indexes (but relhasindex was set) */
        if indexoidlist.is_empty() {
            return Ok(Vec::new());
        }

        /*
         * Copy the rd_pkindex and rd_replidindex values computed by
         * RelationGetIndexList before proceeding. This is needed because a
         * relcache flush could occur inside index_open below, resetting the
         * fields managed by RelationGetIndexList.
         */
        let (relpkindex, relreplindex) =
            with_rel(relation, |rd| (rd.rd_pkindex, rd.rd_replidindex));

        let mut uindexattrs: Vec<i32> = Vec::new();
        let mut pkindexattrs: Vec<i32> = Vec::new();
        let mut idindexattrs: Vec<i32> = Vec::new();
        let mut hotblockingattrs: Vec<i32> = Vec::new();
        let mut summarizedattrs: Vec<i32> = Vec::new();

        for &index_oid in &indexoidlist {
            /*
             * Extract the index's key columns, expression attrs, predicate
             * attrs, indisunique/indnatts/indnkeyatts/amsummarizing via the
             * index_open seam (relation + node owners). Own merging follows.
             */
            let info = open_index_attrs_seam(index_oid)?;

            /* Can this index be referenced by a foreign key? */
            let is_key = info.indisunique && !info.has_expressions && !info.has_predicate;
            /* Is this a primary key? */
            let is_pk = index_oid == relpkindex;
            /* Is this index the configured (or default) replica identity? */
            let is_id_key = index_oid == relreplindex;

            /*
             * If the index is summarizing, it doesn't block HOT updates, but we
             * may still need to update it; decide which bitmap to update.
             */
            let summarizing = info.amsummarizing;

            /* Collect simple attribute references */
            for (i, &attrnum) in info.indkey.iter().enumerate() {
                let attrnum = attrnum as i32;
                if attrnum != 0 {
                    let member = attrnum - FirstLowInvalidHeapAttributeNumber as i32;
                    if summarizing {
                        bms_add_member(&mut summarizedattrs, member);
                    } else {
                        bms_add_member(&mut hotblockingattrs, member);
                    }

                    if is_key && (i as i16) < info.indnkeyatts {
                        bms_add_member(&mut uindexattrs, member);
                    }
                    if is_pk && (i as i16) < info.indnkeyatts {
                        bms_add_member(&mut pkindexattrs, member);
                    }
                    if is_id_key && (i as i16) < info.indnkeyatts {
                        bms_add_member(&mut idindexattrs, member);
                    }
                }
            }

            /* Collect all attributes used in expressions / predicate, too. */
            let dest = if summarizing {
                &mut summarizedattrs
            } else {
                &mut hotblockingattrs
            };
            for &m in &info.expr_attrs {
                bms_add_member(dest, m);
            }
            for &m in &info.pred_attrs {
                bms_add_member(dest, m);
            }
        }

        /*
         * During one of the index_opens above, we might have received a
         * relcache flush event which might signal a change in the rel's index
         * list. If so, start over to deliver up-to-date attribute bitmaps.
         */
        let newindexoidlist = RelationGetIndexList(relation)?;
        let (newpk, newrepl) =
            with_rel(relation, |rd| (rd.rd_pkindex, rd.rd_replidindex));
        if newindexoidlist == indexoidlist && relpkindex == newpk && relreplindex == newrepl {
            /* Still the same index set, so proceed */
            break (
                uindexattrs,
                pkindexattrs,
                idindexattrs,
                hotblockingattrs,
                summarizedattrs,
            );
        }
        /* Gotta do it over. */
    };

    /*
     * Now save copies of the bitmaps in the relcache entry. We intentionally
     * set rd_attrsvalid last.
     */
    with_rel_mut(relation, |rd| {
        rd.rd_attrsvalid = false;
        rd.rd_keyattr = uindexattrs.clone();
        rd.rd_pkattr = pkindexattrs.clone();
        rd.rd_idattr = idindexattrs.clone();
        rd.rd_hotblockingattr = hotblockingattrs.clone();
        rd.rd_summarizedattr = summarizedattrs.clone();
        rd.rd_attrsvalid = true;
    });

    /* We return our original working copy for caller to play with */
    Ok(match attrKind {
        IndexAttrBitmapKind::Keys => uindexattrs,
        IndexAttrBitmapKind::PrimaryKey => pkindexattrs,
        IndexAttrBitmapKind::Identity => idindexattrs,
        IndexAttrBitmapKind::HotBlocking => hotblockingattrs,
        IndexAttrBitmapKind::Summarized => summarizedattrs,
    })
}

/* ==========================================================================
 * RelationGetIdentityKeyBitmap -- replica-identity key columns.
 *
 * Own logic over the store. Forces RelationGetReplicaIndex, opens the identity
 * index descriptor via the in-crate cache (`RelationIdGetRelation`), reads its
 * `rd_index->indkey`, and caches the bitmap on `rd_idattr`. No lock is taken
 * (historic snapshot path), matching C.
 * ======================================================================== */

/// `RelationGetIdentityKeyBitmap(relation)` (relcache.c): the replica-identity
/// index key columns as offset members, or `None` when there is no identity
/// index. **Own logic** (opens the identity index via the in-crate cache).
pub fn RelationGetIdentityKeyBitmap(relation: Oid) -> PgResult<Option<Vec<i32>>> {
    /* Quick exit if we already computed the result */
    if let Some(early) = with_rel(relation, |rd| {
        if !rd.rd_idattr.is_empty() {
            Some(Some(rd.rd_idattr.clone()))
        } else if !rd.rd_rel.relhasindex {
            /* Fast path if definitely no indexes */
            Some(None)
        } else {
            None
        }
    }) {
        return Ok(early);
    }

    /* Historic snapshot must be set (Assert in C; not modeled here). */

    let replidindex = RelationGetReplicaIndex(relation)?;

    /* Fall out if there is no replica identity index */
    if !OidIsValid(replidindex) {
        return Ok(None);
    }

    /* Look up the description for the replica identity index (RAII pin). */
    let index_desc = crate::core_entry_store::RelationRef::open(replidindex)?;

    /* Add referenced attributes to idindexattrs. */
    let mut idindexattrs: Vec<i32> = Vec::new();
    index_desc.with(|idx| {
        let form: &FormPgIndex = idx
            .rd_index
            .as_ref()
            .expect("replica identity index descriptor has rd_index");
        for (i, &attrnum) in form.indkey.iter().enumerate() {
            let attrnum = attrnum as i32;
            /* We don't include non-key columns into idindexattrs bitmaps. */
            if attrnum != 0 && (i as i16) < form.indnkeyatts {
                bms_add_member(
                    &mut idindexattrs,
                    attrnum - FirstLowInvalidHeapAttributeNumber as i32,
                );
            }
        }
    });

    /* RelationClose(indexDesc): drop the relcache reference (guard Drop). */
    drop(index_desc);

    /* Now save copy of the bitmap in the relcache entry. */
    with_rel_mut(relation, |rd| rd.rd_idattr = idindexattrs.clone());

    /* We return our original working copy for caller to play with */
    Ok(Some(idindexattrs))
}

/* ==========================================================================
 * RelationGetExclusionInfo -- exclusion operator/proc/strategy arrays.
 *
 * Own orchestration: the cached quick-exit (copy out of rd_excl*) and the
 * store-into-entry. The pg_constraint scan + conexclop array decode and the
 * `get_opcode`/`get_op_opfamily_strategy` lookups are genuine cross-unit
 * primitives (genam + lsyscache owners), routed through the seam.
 * ======================================================================== */

/// `RelationGetExclusionInfo(indexRelation, ...)` (relcache.c): the exclusion
/// operator/proc/strategy arrays for an exclusion-constraint index.
pub fn RelationGetExclusionInfo(index_relation: Oid) -> PgResult<()> {
    let (indnkeyatts, cached, indrelid, relid) = with_rel(index_relation, |rd| {
        (
            rd.rd_index.as_ref().map(|i| i.indnkeyatts as usize).unwrap_or(0),
            !rd.rd_exclstrats.is_empty(),
            rd.rd_index.as_ref().map(|i| i.indrelid).unwrap_or(InvalidOid),
            rd.rd_id,
        )
    });

    /* Quick exit if we have the data cached already */
    if cached {
        return Ok(());
    }

    /*
     * Search pg_constraint for the constraint associated with the index and
     * resolve the operator/proc/strategy arrays. The scan, the conexclop array
     * decode, and the `get_opcode`/`get_op_opfamily_strategy` lookups are
     * cross-unit primitives (genam + lsyscache owners). The seam returns the
     * three arrays; storing them on the entry is own logic.
     */
    let (ops, procs, strats) = exclusion_info_seam(relid, indrelid, indnkeyatts)?;

    /* Save a copy of the results in the relcache entry. */
    with_rel_mut(index_relation, |rd| {
        rd.rd_exclops = ops;
        rd.rd_exclprocs = procs;
        rd.rd_exclstrats = strats;
    });
    Ok(())
}

/* ==========================================================================
 * RelationBuildPublicationDesc / RelationBuildRuleLock -- publication / rewrite
 * vocabulary owned cross-unit; routed through their seams. The presence flag on
 * the entry (rd_has_pubdesc / rd_has_rules) is own state.
 * ======================================================================== */

/// `RelationBuildPublicationDesc(relation)` (relcache.c): build `rd_pubdesc`
/// from `pg_publication*` (publication vocabulary — seamed where unported).
pub fn RelationBuildPublicationDesc(relation: Oid) -> PgResult<()> {
    publication_desc_seam(relation)
}

/// `RelationBuildRuleLock(relation)` (relcache.c): build `rd_rules` from
/// `pg_rewrite`. Called during build on the not-yet-inserted descriptor, so it
/// takes `&mut RelationData`.
///
/// The full-Query cache-ownership keystone made this REAL. C builds the rule
/// tree in a `rd_rulescxt` child of `CacheMemoryContext` (process-lifetime) and
/// `stringToNode`s each rule's `ev_qual`/`ev_action` into it; the relcache entry
/// (`rd_rules`) then owns whole `Query` trees for the backend's life. Here the
/// orchestration is OWN logic: the `pg_rewrite` scan + per-row `Form` decode is
/// the genuine genam cross-unit primitive (`relcache_scan_pg_rewrite` seam,
/// returning the raw node-string columns), and `stringToNode` is the read.c
/// cross-unit primitive (`string_to_node` seam). The reconstructed `Query`/`Node`
/// trees are allocated in the process-lifetime [`cache_memory_context`] arena —
/// the faithful `CacheMemoryContext` rendering — so they live for the entry's
/// (backend's) lifetime exactly as in C, with no `'mcx` borrow and no registry.
///
/// Mirrors `relcache.c`: scan `pg_rewrite` by `ev_class = rd_id`
/// (`RewriteRelRulesIndexId`), build one [`RewriteRule`] per row
/// (`ruleId`/`event = ev_type - '0'`/`enabled = ev_enabled`/`isInstead`/
/// `qual = stringToNode(ev_qual)`/`actions = stringToNode(ev_action)`), then
/// `qsort` the rules by `ruleId` (`RewriteRuleCompare`) and store the
/// [`RuleLock`] on the entry. An empty scan stores `None` (C `rd_rules = NULL`).
pub fn RelationBuildRuleLock(relation: &mut RelationData) -> PgResult<()> {
    use types_nodes::nodes::{ntag, CmdType, Node};

    let cache_mcx = cache_memory_context();

    // `systable_beginscan(pg_rewrite, RewriteRelRulesIndexId, ev_class = rd_id)`
    // + per-row `GETSTRUCT(Form_pg_rewrite)` + the two node-string columns
    // (`ev_qual`/`ev_action`), all genam-owned catalog vocabulary.
    let scanned = scan_pg_rewrite_seam(relation.rd_id)?;

    // Build the rule list, `stringToNode`-ing the node strings into the cache
    // arena so the resulting `Query`/`Node` trees live for the entry's lifetime.
    let mut rules: mcx::PgVec<'static, RewriteRule> = mcx::PgVec::new_in(cache_mcx);
    rules.try_reserve(scanned.len()).map_err(|_| cache_mcx.oom(scanned.len()))?;

    for row in scanned {
        // `rule->event = ev_type - '0'` — map the `pg_rewrite.ev_type` char to a
        // `CmdType` (`'1'`=SELECT, `'2'`=UPDATE, `'3'`=INSERT, `'4'`=DELETE).
        let event = match row.ev_type {
            b'1' => CmdType::CMD_SELECT,
            b'2' => CmdType::CMD_UPDATE,
            b'3' => CmdType::CMD_INSERT,
            b'4' => CmdType::CMD_DELETE,
            other => {
                return Err(ereport(ERROR)
                    .errmsg_internal(format!(
                        "invalid ev_type '{}' in pg_rewrite for relation {}",
                        other as char, relation.rd_id
                    ))
                    .into_error());
            }
        };

        // `rule->qual = (Node *) stringToNode(ev_qual)` — a single expression
        // node, or NULL for an unconditional rule. `ev_qual` is a non-NULL
        // `pg_node_tree` column whose rendering is the literal `<>` for an
        // unconditional rule, which `stringToNode` resolves to a NULL pointer;
        // use the nullable entry so `<>` yields `None` rather than an error.
        let mut qual = match row.ev_qual {
            Some(text) => read_seam::string_to_node_opt::call(cache_mcx, text.as_str())?,
            None => None,
        };

        // `rule->actions = (List *) stringToNode(ev_action)` — a list of whole
        // `Query` trees. Reconstruct the `List` node, then move each element's
        // `Query` payload into the rule's `actions` (each lives in the cache
        // arena). C keeps a `List *`; the owned model keeps the `Query` values.
        let mut actions: mcx::PgVec<'static, types_nodes::copy_query::Query<'static>> =
            mcx::PgVec::new_in(cache_mcx);
        // `ev_action` deserializes to a `List` of `Query` (the C `List
        // *actions`). An empty action list (INSTEAD NOTHING) renders as `<>`,
        // which `stringToNode` resolves to a NULL pointer — leave `actions`
        // empty in that case. Use the nullable entry so the `<>` rendering is
        // `None` rather than an error.
        let action_node = match row.ev_action {
            Some(text) => read_seam::string_to_node_opt::call(cache_mcx, text.as_str())?,
            None => None,
        };
        if let Some(action_node) = action_node {
            let action_inner = mcx::PgBox::into_inner(action_node);
            match action_inner.node_tag() {
                ntag::T_List => {
                    let elems = action_inner.into_list().unwrap();
                    actions.try_reserve(elems.len()).map_err(|_| cache_mcx.oom(elems.len()))?;
                    for elem in elems {
                        let elem_inner = mcx::PgBox::into_inner(elem);
                        match elem_inner.node_tag() {
                            ntag::T_Query => actions.push(elem_inner.into_query().unwrap()),
                            _ => {
                                return Err(ereport(ERROR)
                                .errmsg_internal(format!(
                                    "pg_rewrite ev_action element is {:?}, expected Query \
                                     (relation {})",
                                    elem_inner.tag(),
                                    relation.rd_id
                                ))
                                .into_error());
                            }
                        }
                    }
                }
                _ => {
                    return Err(ereport(ERROR)
                        .errmsg_internal(format!(
                            "pg_rewrite ev_action is {:?}, expected a List (relation {})",
                            action_inner.tag(),
                            relation.rd_id
                        ))
                        .into_error());
                }
            }
        }

        // Determine the role to perform the rule's permission checks as
        // (relcache.c:855-882). If this is a SELECT rule defining a view, and
        // the view has "security_invoker" set, all permission checks on the
        // relations referred to by the rule are performed as the invoking user
        // (InvalidOid). In all other cases — including non-SELECT rules on a
        // security-invoker view — they are performed as the relation owner.
        //
        // The view "security_invoker" reloption is not carried on the owned
        // RelationData entry built here, so the security-invoker SELECT-view
        // branch (check_as_user = InvalidOid) is not yet expressible; the
        // owner branch is faithful for every non-security-invoker view, which
        // is the default and the common spine.
        let check_as_user = relation.rd_rel.relowner;

        // Scan through the rule's actions and the qual, setting the
        // checkAsUser field on all RTEPermissionInfos. Doing this at rule load
        // (rather than at store) avoids ALTER TABLE OWNER having to rewrite the
        // stored rules. setRuleCheckAsUser is owned by rewriteDefine.c.
        for action in actions.iter_mut() {
            set_rule_check_as_user_seam::call(cache_mcx, action, check_as_user);
        }
        if let Some(q) = qual.as_deref_mut() {
            set_rule_check_as_user_node_seam::call(cache_mcx, q, check_as_user);
        }

        rules.push(RewriteRule {
            ruleId: row.ruleid,
            event,
            enabled: row.ev_enabled,
            isInstead: row.is_instead,
            qual,
            actions,
        });
    }

    if rules.is_empty() {
        // C: an empty scan leaves `rd_rules = NULL` (and the caller flips
        // `relhasrules` off when this happens).
        relation.rd_rules = None;
        return Ok(());
    }

    // `qsort(rules->rules, numlocks, ..., RewriteRuleCompare)` — sort by ruleId
    // so the rule order is stable across rebuilds regardless of scan order.
    rules.sort_by_key(|r| r.ruleId);

    let lock =
        mcx::alloc_in(cache_mcx, RuleLock { rules }).map_err(|_| cache_mcx.oom(0))?;
    relation.rd_rules = Some(lock);
    Ok(())
}

/* ==========================================================================
 * RelationBuildTriggers / SetTriggerFlags -- the relation's TriggerDesc.
 *
 * Logically commands/trigger.c, but the build runs on the not-yet-inserted
 * descriptor during the relcache build (so it takes `&mut RelationData`, like
 * RelationBuildRuleLock) and the `pg_trigger` scan + per-row Form/var-column
 * decode is the genam cross-unit primitive (`relcache_scan_pg_trigger` seam).
 * The assembled TriggerDesc is allocated in the process-lifetime
 * CacheMemoryContext arena (`cache_memory_context`) — the faithful
 * `CopyTriggerDesc(... into CacheMemoryContext)` rendering — so it lives for
 * the entry's (backend's) lifetime with no `'mcx` borrow.
 * ======================================================================== */

/// `SetTriggerFlags(trigdesc, trigger)` (commands/trigger.c): OR the trigger's
/// hint flags into the `TriggerDesc` so the executor can skip searching for a
/// kind of trigger the relation does not have.
fn SetTriggerFlags(trigdesc: &mut types_trigger::TriggerDesc<'static>, tgtype: i16, trigger: &types_trigger::Trigger<'static>) {
    use types_catalog::pg_trigger::{
        TRIGGER_FOR_DELETE, TRIGGER_FOR_INSERT, TRIGGER_FOR_UPDATE, TRIGGER_TYPE_AFTER,
        TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_DELETE, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_INSTEAD,
        TRIGGER_TYPE_MATCHES, TRIGGER_TYPE_ROW, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_TRUNCATE,
        TRIGGER_TYPE_UPDATE,
    };

    trigdesc.trig_insert_before_row |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_INSERT);
    trigdesc.trig_insert_after_row |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_INSERT);
    trigdesc.trig_insert_instead_row |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_INSTEAD, TRIGGER_TYPE_INSERT);
    trigdesc.trig_insert_before_statement |= TRIGGER_TYPE_MATCHES(
        tgtype,
        TRIGGER_TYPE_STATEMENT,
        TRIGGER_TYPE_BEFORE,
        TRIGGER_TYPE_INSERT,
    );
    trigdesc.trig_insert_after_statement |= TRIGGER_TYPE_MATCHES(
        tgtype,
        TRIGGER_TYPE_STATEMENT,
        TRIGGER_TYPE_AFTER,
        TRIGGER_TYPE_INSERT,
    );
    trigdesc.trig_update_before_row |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_UPDATE);
    trigdesc.trig_update_after_row |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_UPDATE);
    trigdesc.trig_update_instead_row |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_INSTEAD, TRIGGER_TYPE_UPDATE);
    trigdesc.trig_update_before_statement |= TRIGGER_TYPE_MATCHES(
        tgtype,
        TRIGGER_TYPE_STATEMENT,
        TRIGGER_TYPE_BEFORE,
        TRIGGER_TYPE_UPDATE,
    );
    trigdesc.trig_update_after_statement |= TRIGGER_TYPE_MATCHES(
        tgtype,
        TRIGGER_TYPE_STATEMENT,
        TRIGGER_TYPE_AFTER,
        TRIGGER_TYPE_UPDATE,
    );
    trigdesc.trig_delete_before_row |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_DELETE);
    trigdesc.trig_delete_after_row |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_DELETE);
    trigdesc.trig_delete_instead_row |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_INSTEAD, TRIGGER_TYPE_DELETE);
    trigdesc.trig_delete_before_statement |= TRIGGER_TYPE_MATCHES(
        tgtype,
        TRIGGER_TYPE_STATEMENT,
        TRIGGER_TYPE_BEFORE,
        TRIGGER_TYPE_DELETE,
    );
    trigdesc.trig_delete_after_statement |= TRIGGER_TYPE_MATCHES(
        tgtype,
        TRIGGER_TYPE_STATEMENT,
        TRIGGER_TYPE_AFTER,
        TRIGGER_TYPE_DELETE,
    );
    // there are no row-level truncate triggers
    trigdesc.trig_truncate_before_statement |= TRIGGER_TYPE_MATCHES(
        tgtype,
        TRIGGER_TYPE_STATEMENT,
        TRIGGER_TYPE_BEFORE,
        TRIGGER_TYPE_TRUNCATE,
    );
    trigdesc.trig_truncate_after_statement |= TRIGGER_TYPE_MATCHES(
        tgtype,
        TRIGGER_TYPE_STATEMENT,
        TRIGGER_TYPE_AFTER,
        TRIGGER_TYPE_TRUNCATE,
    );

    // TRIGGER_USES_TRANSITION_TABLE(name) == (name != NULL).
    let has_new = trigger.tgnewtable.is_some();
    let has_old = trigger.tgoldtable.is_some();
    trigdesc.trig_insert_new_table |= TRIGGER_FOR_INSERT(tgtype) && has_new;
    trigdesc.trig_update_old_table |= TRIGGER_FOR_UPDATE(tgtype) && has_old;
    trigdesc.trig_update_new_table |= TRIGGER_FOR_UPDATE(tgtype) && has_new;
    trigdesc.trig_delete_old_table |= TRIGGER_FOR_DELETE(tgtype) && has_old;
}

/// `RelationBuildTriggers(relation)` (commands/trigger.c): build `rd_trigdesc`
/// from `pg_trigger`. Called during build on the not-yet-inserted descriptor,
/// so it takes `&mut RelationData`.
///
/// C scans `pg_trigger` by `tgrelid = RelationGetRelid` under
/// `TriggerRelidNameIndexId` (name order, so triggers fire in name order),
/// builds a working `Trigger[]`, sets the `TriggerDesc` hint flags via
/// `SetTriggerFlags`, then `CopyTriggerDesc`s the whole thing into
/// `CacheMemoryContext`. Here the `pg_trigger` scan + per-row Form/var-column
/// decode is the genam cross-unit primitive (`relcache_scan_pg_trigger`); the
/// assembled `TriggerDesc` is allocated directly in the process-lifetime
/// [`cache_memory_context`] arena (so the copy-into-cache is implicit — every
/// owned `Trigger` field already lands there). An empty scan stores `None`
/// (the C `numtrigs == 0` path returns without setting `trigdesc`).
pub fn RelationBuildTriggers(relation: &mut RelationData) -> PgResult<()> {
    let cache_mcx = cache_memory_context();

    // `systable_beginscan(pg_trigger, TriggerRelidNameIndexId, tgrelid = rd_id)`
    // + per-row `GETSTRUCT(Form_pg_trigger)` + the four var-width columns
    // (`tgattr`/`tgargs`/`tgqual`/`tgoldtable`/`tgnewtable`), genam-owned.
    let scanned = genam_seam::relcache_scan_pg_trigger::call(relation.rd_id)?;

    // There might not be any triggers (C: pfree(triggers); return;).
    if scanned.is_empty() {
        relation.rd_trigdesc = None;
        return Ok(());
    }

    let numtrigs = scanned.len();
    let mut trigdesc = types_trigger::TriggerDesc::new_in(cache_mcx);
    let mut triggers: mcx::PgVec<'static, types_trigger::Trigger<'static>> =
        mcx::PgVec::new_in(cache_mcx);
    triggers.try_reserve(numtrigs).map_err(|_| cache_mcx.oom(numtrigs))?;

    for row in scanned {
        // build->tgname = nameout(...); the args/attr arrays + qual/transition
        // tables, all copied into the cache arena (CopyTriggerDesc's pstrdups).
        let tgname = mcx::PgString::from_str_in(&row.tgname, cache_mcx)
            .map_err(|_| cache_mcx.oom(row.tgname.len()))?;

        // build->tgnattr = pg_trigger->tgattr.dim1; the int2vector elements.
        let mut tgattr: mcx::PgVec<'static, i16> = mcx::PgVec::new_in(cache_mcx);
        tgattr.try_reserve(row.tgattr.len()).map_err(|_| cache_mcx.oom(row.tgattr.len()))?;
        for &a in &row.tgattr {
            tgattr.push(a);
        }
        let tgnattr = tgattr.len() as i16;

        // build->tgargs[i] = pstrdup(p); one PgString per argument.
        let mut tgargs: mcx::PgVec<'static, mcx::PgString<'static>> = mcx::PgVec::new_in(cache_mcx);
        tgargs.try_reserve(row.tgargs.len()).map_err(|_| cache_mcx.oom(row.tgargs.len()))?;
        for arg in &row.tgargs {
            let s = mcx::PgString::from_str_in(arg, cache_mcx).map_err(|_| cache_mcx.oom(arg.len()))?;
            tgargs.push(s);
        }

        let tgqual = match &row.tgqual {
            Some(q) => Some(
                mcx::PgString::from_str_in(q, cache_mcx).map_err(|_| cache_mcx.oom(q.len()))?,
            ),
            None => None,
        };
        let tgoldtable = match &row.tgoldtable {
            Some(t) => Some(
                mcx::PgString::from_str_in(t, cache_mcx).map_err(|_| cache_mcx.oom(t.len()))?,
            ),
            None => None,
        };
        let tgnewtable = match &row.tgnewtable {
            Some(t) => Some(
                mcx::PgString::from_str_in(t, cache_mcx).map_err(|_| cache_mcx.oom(t.len()))?,
            ),
            None => None,
        };

        triggers.push(types_trigger::Trigger {
            tgoid: row.tgoid,
            tgname,
            tgfoid: row.tgfoid,
            tgtype: row.tgtype,
            tgenabled: row.tgenabled,
            tgisinternal: row.tgisinternal,
            // build->tgisclone = OidIsValid(pg_trigger->tgparentid).
            tgisclone: OidIsValid(row.tgparentid),
            tgconstrrelid: row.tgconstrrelid,
            tgconstrindid: row.tgconstrindid,
            tgconstraint: row.tgconstraint,
            tgdeferrable: row.tgdeferrable,
            tginitdeferred: row.tginitdeferred,
            tgnargs: row.tgnargs,
            tgnattr,
            tgattr,
            tgargs,
            tgqual,
            tgoldtable,
            tgnewtable,
        });
    }

    // for (i = 0; i < numtrigs; i++) SetTriggerFlags(trigdesc, &triggers[i]);
    for trig in triggers.iter() {
        SetTriggerFlags(&mut trigdesc, trig.tgtype, trig);
    }

    trigdesc.numtriggers = numtrigs as i32;
    trigdesc.triggers = triggers;

    let boxed = mcx::alloc_in(cache_mcx, trigdesc).map_err(|_| cache_mcx.oom(0))?;
    relation.rd_trigdesc = Some(boxed);
    Ok(())
}

/* ==========================================================================
 * RelationBuildRowSecurity -- row-security descriptor (rd_rsdesc).
 *
 * C: commands/policy.c:193. Loads the relation's RLS policies from pg_policy
 * and stores the assembled RowSecurityDesc in the relcache entry. The
 * descriptor and its policy expression trees live in a row-security memory
 * context that C reparents under CacheMemoryContext; here the whole descriptor
 * is allocated directly in the process-lifetime `cache_memory_context` arena
 * (so the reparent is implicit — every owned field already lands there).
 *
 * The pg_policy scan + per-row Form/array/text decode is the genam cross-unit
 * primitive (`relcache_scan_pg_policy`, in polname order). The qual texts are
 * reconstructed via `string_to_node` (read-funcs owner) into the same arena;
 * `hassublinks` is computed via `check_expr_has_sub_link` (rewriteManip owner).
 * ======================================================================== */

/// `RelationBuildRowSecurity(relation)` (commands/policy.c): scan `pg_policy`
/// for the relation's policies (name order), build each [`RowSecurityPolicy`]
/// (roles, qual, with-check qual, sublink flag) in the cache arena, and attach
/// the assembled [`RowSecurityDesc`] to `relation.rd_rsdesc`.
///
/// C unconditionally creates a `RowSecurityDesc` (even when the relation has no
/// explicit policies — `RelationBuildRowSecurity` is only called when
/// `relrowsecurity` is set, and an empty descriptor drives the default-deny
/// policy in `get_policies_for_relation`). So an empty scan still installs a
/// `Some(descriptor)` with an empty policy list, matching C.
pub fn RelationBuildRowSecurity(relation: &mut RelationData) -> PgResult<()> {
    let cache_mcx = cache_memory_context();

    // catalog = table_open(PolicyRelationId); systable_beginscan(
    //   PolicyPolrelidPolnameIndexId, polrelid = rd_id); per-row GETSTRUCT +
    // the polroles oid[] decode + the polqual/polwithcheck text reads.
    let scanned = genam_seam::relcache_scan_pg_policy::call(relation.rd_id)?;

    let mut policies: mcx::PgVec<'static, RowSecurityPolicy> = mcx::PgVec::new_in(cache_mcx);
    policies.try_reserve(scanned.len()).map_err(|_| cache_mcx.oom(scanned.len()))?;

    for row in scanned {
        // policy->policy_name = MemoryContextStrdup(rscxt, NameStr(polname)).
        let policy_name = mcx::PgString::from_str_in(&row.polname, cache_mcx)
            .map_err(|_| cache_mcx.oom(row.polname.len()))?;

        // policy->roles = DatumGetArrayTypePCopy(polroles) — decoded Oid[].
        let mut roles: mcx::PgVec<'static, Oid> = mcx::PgVec::new_in(cache_mcx);
        roles.try_reserve(row.polroles.len()).map_err(|_| cache_mcx.oom(row.polroles.len()))?;
        for &r in &row.polroles {
            roles.push(r);
        }

        // policy->qual = (Expr *) stringToNode(TextDatumGetCString(polqual)).
        let qual = match &row.polqual {
            Some(text) => Some(read_seam::string_to_node::call(cache_mcx, text.as_str())?),
            None => None,
        };
        // policy->with_check_qual = stringToNode(polwithcheck).
        let with_check_qual = match &row.polwithcheck {
            Some(text) => Some(read_seam::string_to_node::call(cache_mcx, text.as_str())?),
            None => None,
        };

        // policy->hassublinks = checkExprHasSubLink((Node *) qual) ||
        //                       checkExprHasSubLink((Node *) with_check_qual);
        let hassublinks = qual.as_ref().is_some_and(|q| {
            backend_rewrite_rewritemanip_seams::check_expr_has_sub_link::call(q)
        }) || with_check_qual.as_ref().is_some_and(|q| {
            backend_rewrite_rewritemanip_seams::check_expr_has_sub_link::call(q)
        });

        let policy = RowSecurityPolicy {
            policy_name,
            polcmd: row.polcmd,
            roles,
            permissive: row.polpermissive,
            qual,
            with_check_qual,
            hassublinks,
        };

        // rsdesc->policies = lcons(policy, rsdesc->policies) — built in reverse.
        policies.insert(0, policy);
    }

    let rsdesc = RowSecurityDesc { policies };
    let boxed = mcx::alloc_in(cache_mcx, rsdesc).map_err(|_| cache_mcx.oom(0))?;
    relation.rd_rsdesc = Some(boxed);
    Ok(())
}

/* ==========================================================================
 * Bitmapset helper (Bitmapset over offset members, kept sorted/deduped to
 * match `bms_add_member`/`bms_equal` ordering used by the bitmap builders).
 * ======================================================================== */

/// `bms_add_member(set, member)` over the offset-member `Vec<i32>` model: add
/// `member` if not already present, keeping the set sorted (so two sets that
/// hold the same members compare equal element-wise, mirroring `bms_equal`).
fn bms_add_member(set: &mut Vec<i32>, member: i32) {
    if let Err(pos) = set.binary_search(&member) {
        set.insert(pos, member);
    }
}

/* ==========================================================================
 * Genuine cross-unit primitives (seam-and-panic until the owner lands).
 *
 * Each `*_seam` below is a single genuine cross-unit boundary the derived
 * orchestration calls into: a catalog scan (genam owner), an index open
 * (relation/indexam owner), a node-tree transform (node owner), or a syscache
 * lookup (lsyscache owner). Per "mirror PG and panic", each is a real
 * `seam!()::call` into its owner (panics until the owner installs it) rather
 * than being restructured away; the orchestration above is real and uses their
 * results over the owned store.
 * ======================================================================== */

/// One `pg_index` row as the index scan yields it for `RelationGetIndexList`:
/// the `Form_pg_index` payload plus whether `indpred` is null (`heap_attisnull`
/// over `Anum_pg_index_indpred`).
pub(crate) struct ScannedIndex {
    pub(crate) index: FormPgIndex,
    pub(crate) indpred_isnull: bool,
}

/// `systable_beginscan(pg_index, IndexIndrelidIndexId, indrelid = relid)` then
/// `systable_getnext` (genam owner). Returns each matching row's form + the
/// indpred-isnull flag.
fn scan_pg_index_seam(relid: Oid) -> PgResult<Vec<ScannedIndex>> {
    // The scan + per-row `GETSTRUCT(Form_pg_index)` decode is genam-owned
    // catalog vocabulary; marshal each owner-vocabulary row into the entry's
    // owned `FormPgIndex` shape.
    let rows = genam_seam::relcache_scan_pg_index::call(relid)?;
    Ok(rows
        .into_iter()
        .map(|r| ScannedIndex {
            index: FormPgIndex {
                indexrelid: r.indexrelid,
                indrelid: relid,
                indnatts: r.indnatts,
                indnkeyatts: r.indnkeyatts,
                indisunique: r.indisunique,
                indnullsnotdistinct: r.indnullsnotdistinct,
                indisprimary: r.indisprimary,
                indisexclusion: r.indisexclusion,
                indimmediate: r.indimmediate,
                indisclustered: r.indisclustered,
                indisvalid: r.indisvalid,
                indcheckxmin: r.indcheckxmin,
                indisready: r.indisready,
                indislive: r.indislive,
                indisreplident: r.indisreplident,
                indkey: r.indkey,
            },
            indpred_isnull: r.indpred_isnull,
        })
        .collect())
}

/// `systable_beginscan(pg_statistic_ext, StatisticExtRelidIndexId, stxrelid =
/// relid)` then `systable_getnext` (genam owner). Returns the matching
/// statistics-object OIDs.
fn scan_pg_statistic_ext_seam(relid: Oid) -> PgResult<Vec<Oid>> {
    genam_seam::relcache_scan_pg_statistic_ext::call(relid)
}

/// `systable_beginscan(pg_constraint, conrelid = relid)` then the per-row
/// `ForeignKeyCacheInfo` build via `DeconstructFkConstraintRow` (genam + FK node
/// owners). Returns the assembled FK cache-info rows.
fn scan_pg_constraint_fkeys_seam(relid: Oid) -> PgResult<Vec<ForeignKeyCacheInfo>> {
    let rows = genam_seam::relcache_scan_pg_constraint_fkeys::call(relid)?;
    Ok(rows
        .into_iter()
        .map(|r| ForeignKeyCacheInfo {
            conoid: r.conoid,
            conrelid: r.conrelid,
            confrelid: r.confrelid,
            conenforced: r.conenforced,
            nkeys: r.nkeys,
            conkey: r.conkey,
            confkey: r.confkey,
            conpfeqop: r.conpfeqop,
        })
        .collect())
}

/// `RelationGetFKeyList(relid) != NIL` — does the relation have any foreign-key
/// constraints? A thin bool wrapper so callers can test FK presence without
/// naming the crate-private [`ForeignKeyCacheInfo`] element type.
pub fn relation_has_foreign_keys(relation: Oid) -> PgResult<bool> {
    Ok(!RelationGetFKeyList(relation)?.is_empty())
}

/// `ForeignKeyCacheInfo` (nodes/parsenodes.h) — the FK cache-info the planner
/// (`get_relation_foreign_keys`, plancat.c) reads from `rd_fkeylist`.
#[derive(Clone)]
pub(crate) struct ForeignKeyCacheInfo {
    pub(crate) conoid: Oid,
    pub(crate) conrelid: Oid,
    pub(crate) confrelid: Oid,
    pub(crate) conenforced: bool,
    pub(crate) nkeys: i32,
    pub(crate) conkey: Vec<types_core::primitive::AttrNumber>,
    pub(crate) confkey: Vec<types_core::primitive::AttrNumber>,
    pub(crate) conpfeqop: Vec<Oid>,
}

/// `RelationGetIndexExpressions(relation)`'s node-tree transform: `stringToNode`
/// of `pg_index.indexprs`, `eval_const_expressions`, `fix_opfuncids`. The owner
/// seam returns the decoded list; this presence-only shell (the C's `rd_indexprs`
/// memoization, which the owned entry does not retain) discards it and just
/// acknowledges. Callers that need the tree (the runtime `BuildIndexInfo` path)
/// invoke the typed `relation_get_index_expressions` seam directly.
fn index_expressions_seam(relid: Oid) -> PgResult<()> {
    let scratch = MemoryContext::new("RelationGetIndexExpressions seam");
    nodexform_seam::index_expressions::call(scratch.mcx(), relid)?;
    Ok(())
}

/// `RelationGetIndexPredicate(relation)`'s node-tree transform: `stringToNode`
/// of `pg_index.indpred`, `eval_const_expressions`, `canonicalize_qual`,
/// `make_ands_implicit`, `fix_opfuncids`. Presence-only shell (see
/// [`index_expressions_seam`]).
fn index_predicate_seam(relid: Oid) -> PgResult<()> {
    let scratch = MemoryContext::new("RelationGetIndexPredicate seam");
    nodexform_seam::index_predicate::call(scratch.mcx(), relid)?;
    Ok(())
}

/// `RelationGetDummyIndexExpressions(relation)`'s dummy-Const build: read the
/// raw `pg_index.indexprs` datum (`heap_getattr` over `GetPgIndexDescriptor`),
/// `stringToNode` the expression list, then per sub-tree `makeConst(exprType,
/// exprTypmod, exprCollation, 1, (Datum) 0, true /*isnull*/, true /*byval*/)`.
/// All node vocabulary (`stringToNode`/`makeConst`/`exprType`/`exprTypmod`/
/// `exprCollation`) + the raw `rd_indextuple` read; node owner.
fn dummy_index_expressions_seam(relid: Oid) -> PgResult<()> {
    nodexform_seam::dummy_index_expressions::call(relid)
}

/// One index's attribute contributions for `RelationGetIndexAttrBitmap`,
/// produced by `index_open` (relation owner) + the `pull_varattnos` node pulls
/// over the index's `indexprs`/`indpred` (node owner).
pub(crate) struct IndexAttrInfo {
    pub(crate) indisunique: bool,
    pub(crate) indnkeyatts: i16,
    pub(crate) amsummarizing: bool,
    pub(crate) has_expressions: bool,
    pub(crate) has_predicate: bool,
    /// `rd_index->indkey.values[0..indnatts]` (raw table column numbers).
    pub(crate) indkey: Vec<i16>,
    /// Offset members pulled from the index expressions.
    pub(crate) expr_attrs: Vec<i32>,
    /// Offset members pulled from the index predicate.
    pub(crate) pred_attrs: Vec<i32>,
}

/// `index_open(indexOid, AccessShareLock)` + extract indkey / expression+
/// predicate attrs + `index_close` (relation + node owners).
fn open_index_attrs_seam(index_oid: Oid) -> PgResult<IndexAttrInfo> {
    let info = nodexform_seam::open_index_attrs::call(index_oid)?;
    Ok(IndexAttrInfo {
        indisunique: info.indisunique,
        indnkeyatts: info.indnkeyatts,
        amsummarizing: info.amsummarizing,
        has_expressions: info.has_expressions,
        has_predicate: info.has_predicate,
        indkey: info.indkey,
        expr_attrs: info.expr_attrs,
        pred_attrs: info.pred_attrs,
    })
}

/// `RelationGetExclusionInfo`'s pg_constraint scan + conexclop decode +
/// `get_opcode`/`get_op_opfamily_strategy` (genam + lsyscache owners). Returns
/// `(operators, procs, strategies)`, each `indnkeyatts` long.
fn exclusion_info_seam(
    relid: Oid,
    indrelid: Oid,
    indnkeyatts: usize,
) -> PgResult<(Vec<Oid>, Vec<Oid>, Vec<u16>)> {
    let _ = RegProcedure::default;
    // `relid` is the exclusion index's own OID (the constraint's `conindid`);
    // `indrelid` is the table the index is on (the constraint's `conrelid`).
    let keys = genam_seam::relcache_exclusion_info::call(relid, indrelid, indnkeyatts)?;
    let mut ops = Vec::with_capacity(keys.len());
    let mut procs = Vec::with_capacity(keys.len());
    let mut strats = Vec::with_capacity(keys.len());
    for k in keys {
        ops.push(k.op);
        procs.push(k.proc);
        strats.push(k.strat);
    }
    Ok((ops, procs, strats))
}

/// `RelationBuildPublicationDesc`'s `pg_publication*` traversal (publication
/// owner): build `rd_pubdesc`.
fn publication_desc_seam(relid: Oid) -> PgResult<()> {
    nodexform_seam::publication_desc::call(relid)
}

/// `RelationBuildRuleLock`'s `pg_rewrite` scan (genam owner):
/// `systable_beginscan(pg_rewrite, RewriteRelRulesIndexId, ev_class = relid)` +
/// per-row `GETSTRUCT(Form_pg_rewrite)` + the `ev_qual`/`ev_action` node-string
/// columns. Returns the raw decoded rows; the relcache builder
/// `stringToNode`s the node strings into the cache arena itself (so the cached
/// trees live in `CacheMemoryContext`, not the scan `mcx`).
fn scan_pg_rewrite_seam(relid: Oid) -> PgResult<Vec<genam_seam::ScannedPgRewrite>> {
    genam_seam::relcache_scan_pg_rewrite::call(relid)
}

/* ==========================================================================
 * CacheMemoryContext arena (the full-Query cache-ownership keystone).
 *
 * C's `CacheMemoryContext` is a process-lifetime memory context; the relcache
 * copies cached node trees (rules, index expressions, partition keys, ...) into
 * it (or into children of it) so a long-lived cache entry can own them past any
 * single query. The faithful Rust rendering is a leaked, never-freed
 * `MemoryContext` whose `Mcx<'static>` handle can be cloned freely — exactly the
 * established `TopMemoryContext` pattern (backend-utils-init-postinit leaks its
 * context the same way). Trees allocated here are `'static`: they borrow nothing
 * from a per-query `'mcx`, so a lifetime-free `RelationData` entry may own them.
 *
 * This is NOT a registry or an invented handle — it is the C `CacheMemoryContext`
 * value, leaked once per backend, exactly as the C context is created once and
 * never destroyed for the backend's life.
 * ======================================================================== */

thread_local! {
    /// The process-lifetime `CacheMemoryContext` (`utils/cache/relcache.c` —
    /// `CreateCacheMemoryContext`). Leaked once per backend so the resulting
    /// `Mcx<'static>` outlives every query's `'mcx` arena, exactly like C's
    /// never-freed `CacheMemoryContext`. The relcache entry's owned node trees
    /// (`rd_rules` and, as later campaigns land them, the other node payloads)
    /// allocate here.
    static CACHE_MEMORY_CONTEXT: &'static MemoryContext =
        Box::leak(Box::new(MemoryContext::new("CacheMemoryContext")));
}

/// The process-lifetime `Mcx<'static>` standing in for C's `CacheMemoryContext`.
/// Node trees cached on a relcache entry (which outlives any single query)
/// allocate here so they are `'static` — borrowing nothing from a per-query
/// arena, exactly the C `CacheMemoryContext` lifetime invariant.
pub fn cache_memory_context() -> Mcx<'static> {
    CACHE_MEMORY_CONTEXT.with(|ctx| ctx.mcx())
}

/// `RelationGetIndexAttOptions(relation, copy)` (relcache.c): get/parse the
/// AM/opclass-specific per-column index options into `rd_opcoptions`, caching
/// them in `rd_indexcxt`. **Own logic** (the `index_opclass_options`/
/// `get_attoptions` calls are the cross-unit primitives). Filled with the
/// derived family; `RelationInitIndexAccessInfo` forces a populate via this.
pub fn RelationGetIndexAttOptions(rd: &mut RelationData, _copy: bool) -> PgResult<()> {
    let relid = rd.rd_id;
    // `RelationGetNumberOfAttributes(relation)` — relnatts (see the XXX in C).
    let natts = rd.rd_rel.relnatts as usize;

    // Try to copy cached options. The C `copy` flag only governs whether the
    // caller gets the cache or a fresh copy of the parsed `bytea **`; in this
    // owned model the parsed options are cached on the entry and the seam
    // returns `()`, so a present cache is simply a no-op.
    if rd.rd_opcoptions.is_some() {
        return Ok(());
    }

    // Get and parse opclass options. `palloc0(sizeof(*opts) * natts)` →
    // one `None` (the C NULL element) per attribute.
    let mut opts: Vec<Option<Vec<u8>>> = vec![None; natts];

    let critical_built = crate::core_entry_store::with_state(|st| st.critical_relcaches_built);

    if critical_built && relid != ATTRIBUTE_RELID_NUM_INDEX_ID {
        // DEFERRED (rebuild-target-addressing keystone): on the REBUILD path this
        // is called against an un-inserted `newrel` Box whose arrays may differ
        // from the resident old entry, but the canonical OID-resolving
        // `index_opclass_options` contract resolves `rd_indam` / `rd_support` by
        // OID against the still-resident OLD entry — a pre-existing latent
        // divergence, NOT introduced by the re-sign. Re-signing to the canonical
        // contract here preserves the exact prior behavior (the old bridge ALSO
        // re-resolved by OID). The faithful fix — parsing against the newrel's
        // own arrays — is the rebuild-target-addressing keystone and is out of
        // this lane.
        let scratch = MemoryContext::new("RelationGetIndexAttOptions");
        let smcx = scratch.mcx();

        // Project the OID-resident index entry into a transient cross-unit
        // `Relation` value-slice (the canonical seam reads its `rd_id`).
        let indrel_data = crate::core_entry_store::with_relation(relid, |r| {
            crate::build::project_relation_data(smcx, r)
        })??;
        let indrel = Relation::open(indrel_data, None);

        for (i, slot) in opts.iter_mut().enumerate() {
            let attnum = (i + 1) as AttrNumber;
            // `get_attoptions(relid, i + 1)` — the raw pg_attribute.attoptions
            // reloptions text[] for this column (lsyscache owner).
            let attoptions = nodexform_seam::get_attoptions::call(relid, attnum)?;
            // Reconstruct the `text[]` Datum (mirroring C `get_attoptions`'s
            // palloc'd text[]); `(Datum) 0` when unset.
            let datum = match &attoptions {
                Some(bytes) => Datum::ByRef(mcx::slice_in(smcx, bytes)?),
                None => Datum::null(),
            };
            // `index_opclass_options(relation, i + 1, attoptions, false)` — the
            // canonical AM/opclass-specific parse (indexam owner).
            *slot = indexam_seam::index_opclass_options::call(&indrel, attnum, datum, false)?;
        }
    }

    // Copy parsed options to the cache (C: into `rd_indexcxt`; the owned entry
    // holds them inline).
    rd.rd_opcoptions = Some(opts);

    Ok(())
}

/// The OID-keyed form of [`RelationGetIndexAttOptions`] for the deferred force
/// in `RelationBuildDesc` (build.rs). The opclass-options parse runs AFTER
/// `cache_insert` (build.rs), so the index entry is already cache-resident; this
/// projects a transient [`Relation`] view of it by OID and drives the canonical
/// `index_opclass_options(indrel, attnum, attoptions, validate=false)` indexam
/// contract directly (no divergent relcache-owned bridge seam). The canonical
/// owner resolves everything it needs (`rd_indam` / `rd_support`) by OID through
/// its own short borrows, so the entry must NOT be held borrowed across the seam
/// loop — the projected `Relation` is an mcx-allocated value copy holding no
/// `RefCell` borrow. This reads `relnatts` / the already-cached guard under a
/// short borrow, builds the projection + drives the (unborrowed) loop in a
/// short-lived scratch context, then writes `rd_opcoptions` back under a short
/// borrow.
pub(crate) fn force_index_att_options(relid: Oid) -> PgResult<()> {
    // RelationGetNumberOfAttributes + the present-cache short-circuit.
    let (natts, already) = crate::core_entry_store::with_relation(relid, |rd| {
        (rd.rd_rel.relnatts as usize, rd.rd_opcoptions.is_some())
    })?;
    if already {
        return Ok(());
    }

    // `palloc0(sizeof(*opts) * natts)` → one `None` (the C NULL element) per
    // attribute.
    let mut opts: Vec<Option<Vec<u8>>> = vec![None; natts];
    let critical_built = crate::core_entry_store::with_state(|st| st.critical_relcaches_built);

    if critical_built && relid != ATTRIBUTE_RELID_NUM_INDEX_ID {
        // Short-lived scratch context for the projected index `Relation` and the
        // reconstructed `attoptions` text[] Datum — mirrors C `get_attoptions`
        // returning a palloc'd text[] that is pfree'd after the parse. The whole
        // projection + parse lives here and drops at function end.
        let scratch = MemoryContext::new("force_index_att_options");
        let smcx = scratch.mcx();

        // Project the cache-resident index entry into a transient cross-unit
        // `Relation` value-slice (the canonical seam reads its `rd_id` and
        // resolves `rd_indam` / `rd_support` by OID; it never holds this view).
        // The copy-out is performed under a short borrow and holds no `RefCell`
        // borrow afterward.
        let indrel_data = crate::core_entry_store::with_relation(relid, |r| {
            crate::build::project_relation_data(smcx, r)
        })??;
        let indrel = Relation::open(indrel_data, None);

        for (i, slot) in opts.iter_mut().enumerate() {
            let attnum = (i + 1) as AttrNumber;
            // `get_attoptions(relid, i + 1)` — the raw pg_attribute.attoptions
            // reloptions text[] for this column (lsyscache owner).
            let attoptions = nodexform_seam::get_attoptions::call(relid, attnum)?;
            // Reconstruct the `text[]` Datum the canonical `index_opclass_options`
            // receives by pointer (caller-adaptation, mirroring C
            // `get_attoptions`'s palloc'd text[]): a present option is the flat
            // varlena image (the by-reference arm), an absent one is `(Datum) 0`.
            let datum = match &attoptions {
                Some(bytes) => Datum::ByRef(mcx::slice_in(smcx, bytes)?),
                None => Datum::null(),
            };
            // `index_opclass_options(relation, i + 1, attoptions, false)` — the
            // AM/opclass-specific parse into the binary `bytea` (indexam owner).
            *slot = indexam_seam::index_opclass_options::call(&indrel, attnum, datum, false)?;
        }
    }

    // Copy parsed options to the cache entry.
    crate::core_entry_store::with_relation_mut(relid, |rd| {
        rd.rd_opcoptions = Some(opts);
    })?;

    Ok(())
}

/// `AttributeRelidNumIndexId` (`pg_attribute_relid_attnum_index`) — guards the
/// per-column opclass-option fetch in [`RelationGetIndexAttOptions`] against
/// recursing through the pg_attribute index before the critical relcaches are
/// built.
const ATTRIBUTE_RELID_NUM_INDEX_ID: Oid = 2659;

#[cfg(test)]
mod cache_ownership_keystone_tests {
    //! The full-Query cache-ownership keystone: prove a lifetime-free, long-lived
    //! cache entry can OWN whole `Query` trees by allocating them in the
    //! process-lifetime `cache_memory_context()` arena. The point of the
    //! keystone is that the cached trees are `'static` — they borrow nothing from
    //! any per-query `'mcx`, so a `RelationData` (which has no lifetime) may hold
    //! them for the backend's life, exactly as C copies rule trees into
    //! `CacheMemoryContext`.

    use super::cache_memory_context;
    use crate::core_entry_store::entry::{RewriteRule, RuleLock};
    use types_nodes::copy_query::Query;
    use types_nodes::nodes::CmdType;

    /// The cache arena hands out an `Mcx<'static>`: a `Query` built in it is
    /// `Query<'static>` and can be returned out of the building scope (it does
    /// not borrow any local). This is the soundness the keystone delivers.
    fn build_cached_query(command: CmdType) -> Query<'static> {
        let mcx = cache_memory_context();
        let mut q = Query::new(mcx);
        q.commandType = command;
        q
    }

    #[test]
    fn cache_arena_yields_static_query_trees() {
        // Build several Query trees in separate scopes; they all live in the
        // single process-lifetime arena and survive past the building call.
        let q1 = build_cached_query(CmdType::CMD_SELECT);
        let q2 = build_cached_query(CmdType::CMD_UPDATE);
        assert_eq!(q1.commandType, CmdType::CMD_SELECT);
        assert_eq!(q2.commandType, CmdType::CMD_UPDATE);
    }

    #[test]
    fn rulelock_owns_query_action_trees() {
        // A RewriteRule's `actions` is a PgVec<'static, Query<'static>> in the
        // cache arena — the C `List *actions` of whole Query trees. Assemble a
        // RuleLock holding them, exactly the shape `RelationBuildRuleLock`
        // produces and `RelationData.rd_rules` owns.
        let mcx = cache_memory_context();
        let mut actions = mcx::PgVec::new_in(mcx);
        actions.push(build_cached_query(CmdType::CMD_SELECT));

        let mut rules = mcx::PgVec::new_in(mcx);
        rules.push(RewriteRule {
            ruleId: 12345,
            event: CmdType::CMD_SELECT,
            enabled: b'O',
            isInstead: true,
            qual: None,
            actions,
        });
        let lock: mcx::PgBox<'static, RuleLock> =
            mcx::alloc_in(mcx, RuleLock { rules }).expect("alloc RuleLock in cache arena");

        // A lifetime-free RelationData can own this for the backend's life.
        let mut entry = crate::core_entry_store::entry::RelationData::default();
        entry.rd_rules = Some(lock);

        let rd_rules = entry.rd_rules.as_ref().expect("rd_rules present");
        assert_eq!(rd_rules.rules.len(), 1);
        assert_eq!(rd_rules.rules[0].ruleId, 12345);
        assert!(rd_rules.rules[0].isInstead);
        assert_eq!(rd_rules.rules[0].enabled, b'O');
        assert_eq!(rd_rules.rules[0].actions.len(), 1);
        assert_eq!(rd_rules.rules[0].actions[0].commandType, CmdType::CMD_SELECT);
    }
}
