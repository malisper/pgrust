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
use backend_nodes_read_seams as read_seam;
use backend_utils_cache_relcache_nodexform_seams as nodexform_seam;
use backend_utils_error::{ereport, PgResult};
use mcx::{Mcx, MemoryContext};
use types_core::primitive::{AttrNumber, Oid, RegProcedure};
use types_core::{InvalidOid, OidIsValid};
use types_error::ERROR;
use types_tuple::{
    FirstLowInvalidHeapAttributeNumber, RELKIND_PARTITIONED_TABLE, REPLICA_IDENTITY_DEFAULT,
    REPLICA_IDENTITY_INDEX,
};

use crate::core_entry_store::entry::{FormPgIndex, RelationData, RewriteRule, RuleLock};
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
pub fn RelationGetFKeyList(relation: Oid) -> PgResult<()> {
    /* Quick exit if we already computed the list. */
    let (fkeyvalid, relid) = with_rel(relation, |rd| (rd.rd_fkeyvalid, rd.rd_id));
    if fkeyvalid {
        return Ok(());
    }

    /*
     * Scan pg_constraint for entries having conrelid = this rel, keeping only
     * the foreign keys. The FK-node build (`ForeignKeyCacheInfo` +
     * `DeconstructFkConstraintRow`) is FK node vocabulary owned cross-unit; the
     * seam returns the assembled rows. The orchestration here mirrors C.
     */
    let _fkeys = scan_pg_constraint_fkeys_seam(relid)?;

    /* Now mark the completed list saved in the relcache entry. */
    with_rel_mut(relation, |rd| rd.rd_fkeyvalid = true);
    Ok(())
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
    // Quick-exit / has-no-expressions decisions need the rd_indextuple's
    // `indexprs` datum, whose node-tree transform is node vocabulary owned
    // cross-unit. Route the whole build through the node-tree owner seam.
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
    use types_nodes::nodes::{CmdType, Node};

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
        // node, or NULL for an unconditional rule.
        let qual = match row.ev_qual {
            Some(text) => Some(read_seam::string_to_node::call(cache_mcx, text.as_str())?),
            None => None,
        };

        // `rule->actions = (List *) stringToNode(ev_action)` — a list of whole
        // `Query` trees. Reconstruct the `List` node, then move each element's
        // `Query` payload into the rule's `actions` (each lives in the cache
        // arena). C keeps a `List *`; the owned model keeps the `Query` values.
        let mut actions: mcx::PgVec<'static, types_nodes::copy_query::Query<'static>> =
            mcx::PgVec::new_in(cache_mcx);
        if let Some(text) = row.ev_action {
            let action_node = read_seam::string_to_node::call(cache_mcx, text.as_str())?;
            // `ev_action` deserializes to a `List` of `Query` (the C
            // `List *actions`). An empty action list is a valid INSTEAD NOTHING
            // rule.
            match mcx::PgBox::into_inner(action_node) {
                Node::List(elems) => {
                    actions.try_reserve(elems.len()).map_err(|_| cache_mcx.oom(elems.len()))?;
                    for elem in elems {
                        match mcx::PgBox::into_inner(elem) {
                            Node::Query(q) => actions.push(q),
                            other => {
                                return Err(ereport(ERROR)
                                .errmsg_internal(format!(
                                    "pg_rewrite ev_action element is {:?}, expected Query \
                                     (relation {})",
                                    other.tag(),
                                    relation.rd_id
                                ))
                                .into_error());
                            }
                        }
                    }
                }
                other => {
                    return Err(ereport(ERROR)
                        .errmsg_internal(format!(
                            "pg_rewrite ev_action is {:?}, expected a List (relation {})",
                            other.tag(),
                            relation.rd_id
                        ))
                        .into_error());
                }
            }
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
        .map(|r| ForeignKeyCacheInfo { conoid: r.conoid })
        .collect())
}

/// `ForeignKeyCacheInfo` (nodes/parsenodes.h) — FK node vocabulary owned
/// cross-unit; opaque to the derived orchestration (it only stores the list).
pub(crate) struct ForeignKeyCacheInfo {
    #[allow(dead_code)]
    pub(crate) conoid: Oid,
}

/// `RelationGetIndexExpressions(relation)`'s node-tree transform: `stringToNode`
/// of `pg_index.indexprs`, `eval_const_expressions`, `fix_opfuncids`, then cache
/// into `rd_indexprs` (node owner).
fn index_expressions_seam(relid: Oid) -> PgResult<()> {
    nodexform_seam::index_expressions::call(relid)
}

/// `RelationGetIndexPredicate(relation)`'s node-tree transform: `stringToNode`
/// of `pg_index.indpred`, `eval_const_expressions`, `canonicalize_qual`,
/// `make_ands_implicit`, `fix_opfuncids`, then cache into `rd_indpred`.
fn index_predicate_seam(relid: Oid) -> PgResult<()> {
    nodexform_seam::index_predicate::call(relid)
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
    for (i, slot) in opts.iter_mut().enumerate() {
        // `criticalRelcachesBuilt && relid != AttributeRelidNumIndexId` — avoid
        // recursing through the pg_attribute index's own opclass options before
        // the critical relcaches are built.
        if critical_built && relid != ATTRIBUTE_RELID_NUM_INDEX_ID {
            let attnum = (i + 1) as AttrNumber;
            // `get_attoptions(relid, i + 1)` — the raw pg_attribute.attoptions
            // reloptions for this column (lsyscache owner).
            let attoptions = nodexform_seam::get_attoptions::call(relid, attnum)?;
            // `index_opclass_options(relation, i + 1, attoptions, false)` — the
            // AM/opclass-specific parse into the binary `bytea` (indexam owner).
            *slot = nodexform_seam::index_opclass_options::call(relid, attnum, attoptions)?;
        }
    }

    // Copy parsed options to the cache (C: into `rd_indexcxt`; the owned entry
    // holds them inline).
    rd.rd_opcoptions = Some(opts);

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
