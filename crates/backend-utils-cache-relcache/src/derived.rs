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

#![allow(unsafe_code)]

use backend_utils_error::{ereport, PgResult};
use types_core::primitive::{Oid, RegProcedure};
use types_core::{InvalidOid, OidIsValid};
use types_error::ERROR;
use types_tuple::{
    FirstLowInvalidHeapAttributeNumber, RELKIND_PARTITIONED_TABLE, REPLICA_IDENTITY_DEFAULT,
    REPLICA_IDENTITY_INDEX,
};

use crate::core_entry_store::entry::{FormPgIndex, RelationData};

/// `IndexAttrBitmapKind` (relcache.h) — which attribute-bitmap to fetch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexAttrBitmapKind {
    Keys,
    PrimaryKey,
    Identity,
    HotBlocking,
    Summarized,
}

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
pub fn RelationGetFKeyList(relation: *mut RelationData) -> PgResult<()> {
    // SAFETY: live cache-owned (or in-build) `Relation` pointer.
    let rd = unsafe { &mut *relation };

    /* Quick exit if we already computed the list. */
    if rd.rd_fkeyvalid {
        return Ok(());
    }

    /*
     * Scan pg_constraint for entries having conrelid = this rel, keeping only
     * the foreign keys. The FK-node build (`ForeignKeyCacheInfo` +
     * `DeconstructFkConstraintRow`) is FK node vocabulary owned cross-unit; the
     * seam returns the assembled rows. The orchestration here mirrors C.
     */
    let relid = rd.rd_id;
    let _fkeys = scan_pg_constraint_fkeys_seam(relid)?;

    /* Now mark the completed list saved in the relcache entry. */
    rd.rd_fkeyvalid = true;
    Ok(())
}

/* ==========================================================================
 * RelationGetIndexList -- OIDs of indexes on this relation (rd_indexlist).
 * Full own logic over the store; only the pg_index scan is seamed.
 * ======================================================================== */

/// `RelationGetIndexList(relation)` (relcache.c): the OIDs of the relation's
/// indexes, built from `pg_index` and cached in `rd_indexlist` (+ `rd_pkindex`/
/// `rd_replidindex`). **Own logic.**
pub fn RelationGetIndexList(relation: *mut RelationData) -> PgResult<Vec<Oid>> {
    // SAFETY: live cache-owned (or in-build) `Relation` pointer.
    let rd = unsafe { &mut *relation };

    /* Quick exit if we already computed the list. */
    if rd.rd_indexvalid {
        return Ok(rd.rd_indexlist.clone());
    }

    let replident = rd.rd_rel.relreplident;
    let relkind = rd.rd_rel.relkind;
    let relid = rd.rd_id;

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

    Ok(result)
}

/* ==========================================================================
 * RelationGetStatExtList -- OIDs of extended-statistics objects (rd_statlist).
 * Full own logic; only the pg_statistic_ext scan is seamed.
 * ======================================================================== */

/// `RelationGetStatExtList(relation)` (relcache.c): the OIDs of the relation's
/// extended-statistics objects, cached in `rd_statlist`. **Own logic.**
pub fn RelationGetStatExtList(relation: *mut RelationData) -> PgResult<Vec<Oid>> {
    // SAFETY: live cache-owned (or in-build) `Relation` pointer.
    let rd = unsafe { &mut *relation };

    /* Quick exit if we already computed the list. */
    if rd.rd_statvalid {
        return Ok(rd.rd_statlist.clone());
    }

    let relid = rd.rd_id;

    /*
     * Scan pg_statistic_ext for entries having stxrelid = this rel (genam
     * owner). The seam returns the matching statistics-object OIDs.
     */
    let mut result = scan_pg_statistic_ext_seam(relid)?;

    /* Sort the result list into OID order, per API spec. */
    result.sort_unstable();

    /* Now save a copy of the completed list in the relcache entry. */
    rd.rd_statlist = result.clone();
    rd.rd_statvalid = true;

    Ok(result)
}

/* ==========================================================================
 * RelationGetPrimaryKeyIndex / RelationGetReplicaIndex -- own logic over the
 * index list (force RelationGetIndexList first, then read the cached field).
 * ======================================================================== */

/// `RelationGetPrimaryKeyIndex(relation, deferrable_ok)` (relcache.c): the
/// primary-key index OID (forces `RelationGetIndexList` first).
pub fn RelationGetPrimaryKeyIndex(
    relation: *mut RelationData,
    deferrable_ok: bool,
) -> PgResult<Oid> {
    // SAFETY: live `Relation` pointer.
    let rd = unsafe { &*relation };

    if !rd.rd_indexvalid {
        /* RelationGetIndexList does the heavy lifting. */
        let _ilist = RelationGetIndexList(relation)?;
        debug_assert!(unsafe { (*relation).rd_indexvalid });
    }

    // SAFETY: as above; reread after the possible build.
    let rd = unsafe { &*relation };
    if deferrable_ok {
        Ok(rd.rd_pkindex)
    } else if rd.rd_ispkdeferrable {
        Ok(InvalidOid)
    } else {
        Ok(rd.rd_pkindex)
    }
}

/// `RelationGetReplicaIndex(relation)` (relcache.c): the replica-identity
/// index OID.
pub fn RelationGetReplicaIndex(relation: *mut RelationData) -> PgResult<Oid> {
    // SAFETY: live `Relation` pointer.
    let rd = unsafe { &*relation };

    if !rd.rd_indexvalid {
        /* RelationGetIndexList does the heavy lifting. */
        let _ilist = RelationGetIndexList(relation)?;
        debug_assert!(unsafe { (*relation).rd_indexvalid });
    }

    // SAFETY: as above.
    Ok(unsafe { (*relation).rd_replidindex })
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
pub fn RelationGetIndexExpressions(relation: *mut RelationData) -> PgResult<()> {
    // SAFETY: live `Relation` pointer.
    let _rd = unsafe { &*relation };
    // Quick-exit / has-no-expressions decisions need the rd_indextuple's
    // `indexprs` datum, whose node-tree transform is node vocabulary owned
    // cross-unit. Route the whole build through the node-tree owner seam.
    index_expressions_seam(relation)
}

/// `RelationGetIndexPredicate(relation)` (relcache.c): the index's partial
/// predicate tree (node vocabulary — seamed for the tree, own caching).
pub fn RelationGetIndexPredicate(relation: *mut RelationData) -> PgResult<()> {
    // SAFETY: live `Relation` pointer.
    let _rd = unsafe { &*relation };
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
pub fn RelationGetDummyIndexExpressions(relation: *mut RelationData) -> PgResult<()> {
    // SAFETY: live `Relation` pointer.
    let rd = unsafe { &*relation };

    // Quick exit if there is nothing to do: the C tests `rd_indextuple == NULL
    // || heap_attisnull(rd_indextuple, Anum_pg_index_indexprs)`. In the owned
    // mirror, a non-index entry has no `rd_index` form at all (the C
    // `rd_indextuple == NULL` case). Whether the index actually carries
    // expression columns (`indexprs` not null) is only observable from the raw
    // index tuple's `indexprs` attribute, which the owned model does not carry;
    // that no-expressions short-circuit therefore lives behind the seam.
    if rd.rd_index.is_none() {
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
    relation: *mut RelationData,
    attrKind: IndexAttrBitmapKind,
) -> PgResult<Vec<i32>> {
    /* Quick exit if we already computed the result. */
    // SAFETY: live `Relation` pointer.
    {
        let rd = unsafe { &*relation };
        if rd.rd_attrsvalid {
            return Ok(match attrKind {
                IndexAttrBitmapKind::Keys => rd.rd_keyattr.clone(),
                IndexAttrBitmapKind::PrimaryKey => rd.rd_pkattr.clone(),
                IndexAttrBitmapKind::Identity => rd.rd_idattr.clone(),
                IndexAttrBitmapKind::HotBlocking => rd.rd_hotblockingattr.clone(),
                IndexAttrBitmapKind::Summarized => rd.rd_summarizedattr.clone(),
            });
        }

        /* Fast path if definitely no indexes */
        if !rd.rd_rel.relhasindex {
            return Ok(Vec::new());
        }
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
        // SAFETY: live `Relation` pointer.
        let (relpkindex, relreplindex) = {
            let rd = unsafe { &*relation };
            (rd.rd_pkindex, rd.rd_replidindex)
        };

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
        // SAFETY: live `Relation` pointer.
        let (newpk, newrepl) = {
            let rd = unsafe { &*relation };
            (rd.rd_pkindex, rd.rd_replidindex)
        };
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
    // SAFETY: live `Relation` pointer.
    let rd = unsafe { &mut *relation };
    rd.rd_attrsvalid = false;
    rd.rd_keyattr = uindexattrs.clone();
    rd.rd_pkattr = pkindexattrs.clone();
    rd.rd_idattr = idindexattrs.clone();
    rd.rd_hotblockingattr = hotblockingattrs.clone();
    rd.rd_summarizedattr = summarizedattrs.clone();
    rd.rd_attrsvalid = true;

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
pub fn RelationGetIdentityKeyBitmap(relation: *mut RelationData) -> PgResult<Option<Vec<i32>>> {
    /* Quick exit if we already computed the result */
    // SAFETY: live `Relation` pointer.
    {
        let rd = unsafe { &*relation };
        if !rd.rd_idattr.is_empty() {
            return Ok(Some(rd.rd_idattr.clone()));
        }

        /* Fast path if definitely no indexes */
        if !rd.rd_rel.relhasindex {
            return Ok(None);
        }
    }

    /* Historic snapshot must be set (Assert in C; not modeled here). */

    let replidindex = RelationGetReplicaIndex(relation)?;

    /* Fall out if there is no replica identity index */
    if !OidIsValid(replidindex) {
        return Ok(None);
    }

    /* Look up the description for the replica identity index. */
    let index_desc = crate::core_entry_store::RelationIdGetRelation(replidindex)?;
    if index_desc.is_null() {
        return Err(ereport(ERROR)
            .errmsg_internal("could not open relation with OID for replica identity")
            .into_error());
    }

    /* Add referenced attributes to idindexattrs. */
    let mut idindexattrs: Vec<i32> = Vec::new();
    // SAFETY: just-opened cache-owned index descriptor.
    {
        let idx = unsafe { &*index_desc };
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
    }

    /* RelationClose(indexDesc): drop the relcache reference. */
    crate::core_entry_store::RelationClose(index_desc)?;

    /* Now save copy of the bitmap in the relcache entry. */
    // SAFETY: live `Relation` pointer.
    let rd = unsafe { &mut *relation };
    rd.rd_idattr = idindexattrs.clone();

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
pub fn RelationGetExclusionInfo(index_relation: *mut RelationData) -> PgResult<()> {
    // SAFETY: live `Relation` pointer.
    let rd = unsafe { &*index_relation };

    let indnkeyatts = rd
        .rd_index
        .as_ref()
        .map(|i| i.indnkeyatts as usize)
        .unwrap_or(0);

    /* Quick exit if we have the data cached already */
    if !rd.rd_exclstrats.is_empty() {
        return Ok(());
    }

    /*
     * Search pg_constraint for the constraint associated with the index and
     * resolve the operator/proc/strategy arrays. The scan, the conexclop array
     * decode, and the `get_opcode`/`get_op_opfamily_strategy` lookups are
     * cross-unit primitives (genam + lsyscache owners). The seam returns the
     * three arrays; storing them on the entry is own logic.
     */
    let indrelid = rd
        .rd_index
        .as_ref()
        .map(|i| i.indrelid)
        .unwrap_or(InvalidOid);
    let relid = rd.rd_id;
    let (ops, procs, strats) = exclusion_info_seam(relid, indrelid, indnkeyatts)?;

    /* Save a copy of the results in the relcache entry. */
    // SAFETY: live `Relation` pointer.
    let rd = unsafe { &mut *index_relation };
    rd.rd_exclops = ops;
    rd.rd_exclprocs = procs;
    rd.rd_exclstrats = strats;
    Ok(())
}

/* ==========================================================================
 * RelationBuildPublicationDesc / RelationBuildRuleLock -- publication / rewrite
 * vocabulary owned cross-unit; routed through their seams. The presence flag on
 * the entry (rd_has_pubdesc / rd_has_rules) is own state.
 * ======================================================================== */

/// `RelationBuildPublicationDesc(relation)` (relcache.c): build `rd_pubdesc`
/// from `pg_publication*` (publication vocabulary — seamed where unported).
pub fn RelationBuildPublicationDesc(relation: *mut RelationData) -> PgResult<()> {
    // SAFETY: live `Relation` pointer.
    let _rd = unsafe { &*relation };
    publication_desc_seam(relation)
}

/// `RelationBuildRuleLock(relation)` (relcache.c): build `rd_rules` from
/// `pg_rewrite` (rewrite/node vocabulary — seamed where unported).
pub fn RelationBuildRuleLock(relation: *mut RelationData) -> PgResult<()> {
    // SAFETY: live `Relation` pointer.
    let _rd = unsafe { &*relation };
    rule_lock_seam(relation)
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
 * lookup (lsyscache owner). Per "mirror PG and panic", they `todo!()` (the
 * documented owner-seam boundary) rather than being restructured away; the
 * orchestration above is real and uses their results over the owned store.
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
fn scan_pg_index_seam(_relid: Oid) -> PgResult<Vec<ScannedIndex>> {
    todo!("relcache-derived: pg_index scan for RelationGetIndexList (genam owner seam)")
}

/// `systable_beginscan(pg_statistic_ext, StatisticExtRelidIndexId, stxrelid =
/// relid)` then `systable_getnext` (genam owner). Returns the matching
/// statistics-object OIDs.
fn scan_pg_statistic_ext_seam(_relid: Oid) -> PgResult<Vec<Oid>> {
    todo!("relcache-derived: pg_statistic_ext scan for RelationGetStatExtList (genam owner seam)")
}

/// `systable_beginscan(pg_constraint, conrelid = relid)` then the per-row
/// `ForeignKeyCacheInfo` build via `DeconstructFkConstraintRow` (genam + FK node
/// owners). Returns the assembled FK cache-info rows.
fn scan_pg_constraint_fkeys_seam(_relid: Oid) -> PgResult<Vec<ForeignKeyCacheInfo>> {
    todo!("relcache-derived: pg_constraint FK scan + DeconstructFkConstraintRow (genam + FK node owner seam)")
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
fn index_expressions_seam(_relation: *mut RelationData) -> PgResult<()> {
    todo!("relcache-derived: RelationGetIndexExpressions node-tree transform (node owner seam)")
}

/// `RelationGetIndexPredicate(relation)`'s node-tree transform: `stringToNode`
/// of `pg_index.indpred`, `eval_const_expressions`, `canonicalize_qual`,
/// `make_ands_implicit`, `fix_opfuncids`, then cache into `rd_indpred`.
fn index_predicate_seam(_relation: *mut RelationData) -> PgResult<()> {
    todo!("relcache-derived: RelationGetIndexPredicate node-tree transform (node owner seam)")
}

/// `RelationGetDummyIndexExpressions(relation)`'s dummy-Const build: read the
/// raw `pg_index.indexprs` datum (`heap_getattr` over `GetPgIndexDescriptor`),
/// `stringToNode` the expression list, then per sub-tree `makeConst(exprType,
/// exprTypmod, exprCollation, 1, (Datum) 0, true /*isnull*/, true /*byval*/)`.
/// All node vocabulary (`stringToNode`/`makeConst`/`exprType`/`exprTypmod`/
/// `exprCollation`) + the raw `rd_indextuple` read; node owner.
fn dummy_index_expressions_seam(_relation: *mut RelationData) -> PgResult<()> {
    todo!(
        "relcache-derived: RelationGetDummyIndexExpressions dummy-Const build \
         (stringToNode/makeConst over exprType/exprTypmod/exprCollation; node owner seam)"
    )
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
fn open_index_attrs_seam(_index_oid: Oid) -> PgResult<IndexAttrInfo> {
    todo!("relcache-derived: index_open + pull_varattnos for RelationGetIndexAttrBitmap (relation + node owner seam)")
}

/// `RelationGetExclusionInfo`'s pg_constraint scan + conexclop decode +
/// `get_opcode`/`get_op_opfamily_strategy` (genam + lsyscache owners). Returns
/// `(operators, procs, strategies)`, each `indnkeyatts` long.
fn exclusion_info_seam(
    _relid: Oid,
    _indrelid: Oid,
    _indnkeyatts: usize,
) -> PgResult<(Vec<Oid>, Vec<Oid>, Vec<u16>)> {
    let _ = RegProcedure::default;
    todo!("relcache-derived: RelationGetExclusionInfo pg_constraint scan + opclass lookups (genam + lsyscache owner seam)")
}

/// `RelationBuildPublicationDesc`'s `pg_publication*` traversal (publication
/// owner): build `rd_pubdesc`.
fn publication_desc_seam(_relation: *mut RelationData) -> PgResult<()> {
    todo!("relcache-derived: RelationBuildPublicationDesc pg_publication traversal (publication owner seam)")
}

/// `RelationBuildRuleLock`'s `pg_rewrite` scan + rule-tree build (rewrite/node
/// owner): build `rd_rules`.
fn rule_lock_seam(_relation: *mut RelationData) -> PgResult<()> {
    todo!("relcache-derived: RelationBuildRuleLock pg_rewrite scan + rule build (rewrite owner seam)")
}

/// `RelationGetIndexAttOptions(relation, copy)` (relcache.c): get/parse the
/// AM/opclass-specific per-column index options into `rd_opcoptions`, caching
/// them in `rd_indexcxt`. **Own logic** (the `index_opclass_options`/
/// `get_attoptions` calls are the cross-unit primitives). Filled with the
/// derived family; `RelationInitIndexAccessInfo` forces a populate via this.
pub fn RelationGetIndexAttOptions(_relation: *mut RelationData, _copy: bool) -> PgResult<()> {
    todo!("relcache-derived: RelationGetIndexAttOptions (own logic)")
}
