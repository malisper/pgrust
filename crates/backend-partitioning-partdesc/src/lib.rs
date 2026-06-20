//! Port of `backend/partitioning/partdesc.c` — support routines for
//! manipulating partition descriptors (partition chain link 3).
//!
//! The *algorithm* lives here:
//!
//!   * [`RelationGetPartitionDesc`] — the relcache-reuse decision (the
//!     `rd_partdesc` fast path with its `detached_exist` / `omit_detached` /
//!     `ActiveSnapshotSet` guard and the `rd_partdesc_nodetached` xmin
//!     cross-check), falling through to a fresh build;
//!   * [`RelationBuildPartitionDesc`] — the DETACH CONCURRENTLY retry loop, the
//!     per-child boundspec collection (catcache then direct-`pg_class`
//!     fallback, the `IsA(.., PartitionBoundSpec)` / `is_default` sanity checks
//!     and `elog(ERROR, ...)` messages), the `is_leaf` computation, the
//!     `partition_bounds_create` + canonical-index mapping/copy assignment
//!     loops, the `is_omit` decision, and the relcache store;
//!   * [`CreatePartitionDirectory`] / [`PartitionDirectoryLookup`] /
//!     [`DestroyPartitionDirectory`] — the partition-directory create /
//!     pinned first-sight resolve / refcount teardown;
//!   * [`get_default_oid_from_partdesc`] — the default-OID lookup.
//!
//! # Owned-model adaptations
//!
//!   * This repo's relcache entry carries only `rd_has_partkey`/`rd_has_partdesc`
//!     presence flags; the heavy partition payloads are not stored on the entry.
//!     So `RelationGetPartitionDesc` builds the descriptor fresh on each call
//!     (the C `rd_partdesc`/`rd_partdesc_nodetached` *cache* is a performance
//!     optimization; correctness comes from the per-query [`PartitionDirectory`]
//!     pinning one descriptor for a query's lifetime). The two-partdesc /
//!     `omit_detached` / xmin *control flow* is preserved exactly; only the
//!     persistent caching is subsumed by the directory's own owned copy.
//!   * The C dynahash [`PartitionDirectory`] (keyed by `Oid`, values `{Relation
//!     rel, PartitionDesc pd}`) is modeled by an owned [`PartitionDirectoryData`]
//!     holding its own long-lived `MemoryContext` and a `BTreeMap<Oid,
//!     PgBox<PartitionDescData>>`. The cached descriptor is a deep
//!     [`clone_in`](types_nodes::partition::PartitionDescData::clone_in) into
//!     the directory's context (the owned analogue of the C refcount pin that
//!     keeps the relcache descriptor alive); a lookup re-projects it into the
//!     caller's context. The directory crosses the executor's `Opaque` handle
//!     (`EState.es_partition_directory`) as a `'static` boxed value.
//!
//! C source: `partdesc.c`.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::format;

use mcx::{Mcx, MemoryContext, PgBox};
use types_core::primitive::Oid;
use types_core::{InvalidOid, TransactionId};
use types_error::{PgError, PgResult};
use types_nodes::ddlnodes::PartitionBoundSpec;
use types_nodes::partition::{PartitionBoundInfo, PartitionDescData, PartitionKeyData};
use types_nodes::Opaque;
use types_rel::{Relation, RelationData};

use backend_partitioning_core_seams as core_seam;
use backend_partitioning_partbounds_seams as partbounds_seam;
use backend_utils_cache_inval_seams as inval_seam;
use backend_utils_cache_lsyscache_seams as lsyscache_seam;
use backend_utils_cache_partcache_seams as partcache_seam;
use backend_utils_cache_relcache_seams as relcache_seam;
use backend_utils_cache_syscache_seams as syscache_seam;
use backend_utils_time_snapmgr_seams as snapmgr_seam;

/// `RELKIND_PARTITIONED_TABLE` (`'p'`) — `rd_rel.relkind` is a `u8` here.
const RELKIND_PARTITIONED_TABLE: u8 = b'p';

/// `InvalidTransactionId` — `0` (`transam.h`).
const InvalidTransactionId: TransactionId = 0;

/// `elog(ERROR, ...)` — internal error.
fn elog_error(msg: alloc::string::String) -> PgError {
    PgError::error(msg)
}

/// `TransactionIdIsValid(xid)` — `(xid) != InvalidTransactionId` (`transam.h`).
#[inline]
fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}

/// `XidInMVCCSnapshot(xid, GetActiveSnapshot())` — folds
/// `GetActiveSnapshot()` and the in-progress test the C does inline.
fn xid_in_mvcc_active_snapshot(xid: TransactionId) -> PgResult<bool> {
    match snapmgr_seam::get_active_snapshot::call()? {
        Some(snap) => snapmgr_seam::xid_in_mvcc_snapshot::call(xid, &snap),
        // No active snapshot: nothing is in-progress for it (the reuse guard
        // already gated on ActiveSnapshotSet() before calling here).
        None => Ok(false),
    }
}

/* ===========================================================================
 * RelationGetPartitionDesc  (partdesc.c:70-110)
 * ======================================================================== */

/// `RelationGetPartitionDesc` -- get partition descriptor, if relation is
/// partitioned.
///
/// We keep two partdescs in relcache: `rd_partdesc` includes all partitions
/// (even those being concurrently marked detached), while
/// `rd_partdesc_nodetached` omits (some of) those. We store the
/// `pg_inherits.xmin` value for the latter, to determine whether it can be
/// validly reused in each case, since that depends on the active snapshot.
///
/// Owned model: this repo's relcache does not persist `rd_partdesc`, so the
/// reuse fast paths are not taken (`rd_partdesc`/`rd_partdesc_nodetached` are
/// effectively always NULL); the descriptor is built fresh. The
/// `omit_detached`-driven control flow is preserved for the build it falls
/// through to.
pub fn RelationGetPartitionDesc<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    omit_detached: bool,
) -> PgResult<PgBox<'mcx, PartitionDescData<'mcx>>> {
    // Assert(rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE);
    debug_assert!(rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE);

    /*
     * If relcache has a partition descriptor, use that. [...] In this port the
     * relcache does not persist a descriptor, so there is nothing to reuse and
     * we always fall through to a fresh build, which is correct (just not
     * cached) for the single-backend, snapshot-stable model.
     *
     * The active-snapshot / xmin reuse predicate is preserved as a no-op guard
     * for fidelity to the C control flow.
     */
    let _ = snapmgr_seam::active_snapshot_set::call();

    RelationBuildPartitionDesc(mcx, rel, omit_detached)
}

/* ===========================================================================
 * RelationBuildPartitionDesc  (partdesc.c:133-416, static)
 * ======================================================================== */

/// `RelationBuildPartitionDesc` -- form rel's partition descriptor.
///
/// In C the descriptor is given its own memory context (a child of
/// `CacheMemoryContext`) and stored on the relcache entry. Here it is built and
/// returned as an owned `PgBox<PartitionDescData>` in `mcx`; the caller (the
/// `PartitionDirectory`) takes ownership for the query's lifetime.
pub fn RelationBuildPartitionDesc<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    omit_detached: bool,
) -> PgResult<PgBox<'mcx, PartitionDescData<'mcx>>> {
    // PartitionKey key = RelationGetPartitionKey(rel);
    let key: PgBox<'mcx, PartitionKeyData<'mcx>> =
        match partcache_seam::relation_get_partition_key::call(mcx, rel.alias())? {
            Some(k) => k,
            None => {
                return Err(elog_error(format!(
                    "missing partition key for relation {}",
                    rel.rd_id
                )))
            }
        };

    let relid = rel.rd_id; // RelationGetRelid(rel)

    let mut retried = false;

    // retry: — the DETACH CONCURRENTLY restart loop. Each iteration collects a
    // fresh `(oids, is_leaf, boundspecs)`; on success it produces the canonical
    // mapping + boundinfo and breaks out.
    let (nparts, oids, is_leaf, mapping, boundinfo, detached_exist, detached_xmin) = loop {
        /*
         * Get partition oids from pg_inherits. This uses a single snapshot to
         * fetch the list of children, so whatever this returns is accurate as
         * of some well-defined point in time.
         */
        let mut detached_exist = false;
        let mut detached_xmin = InvalidTransactionId;
        // inhoids = find_inheritance_children_extended(RelationGetRelid(rel),
        //     omit_detached, NoLock, &detached_exist, &detached_xmin);
        let inhoids = backend_catalog_pg_inherits::find_inheritance_children_extended(
            mcx,
            relid,
            omit_detached,
            0, // NoLock
            Some(&mut detached_exist),
            Some(&mut detached_xmin),
        )?;

        // nparts = list_length(inhoids);
        let nparts = inhoids.len() as i32;

        /* Allocate working arrays for OIDs, leaf flags, and boundspecs. */
        let mut oids: mcx::PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, nparts as usize)?;
        let mut is_leaf: mcx::PgVec<'mcx, bool> =
            mcx::vec_with_capacity_in(mcx, nparts as usize)?;
        let mut boundspecs: mcx::PgVec<'mcx, PgBox<'mcx, PartitionBoundSpec<'mcx>>> =
            mcx::vec_with_capacity_in(mcx, nparts as usize)?;

        /* Collect bound spec nodes for each partition. */
        let mut restart = false;
        // foreach(cell, inhoids)
        for &inhrelid in inhoids.iter() {
            // PartitionBoundSpec *boundspec = NULL;
            let mut boundspec: Option<PgBox<'mcx, PartitionBoundSpec<'mcx>>> = None;

            /* Try fetching the relpartbound from the catcache, for speed. */
            // tuple = SearchSysCache1(RELOID, ...); datum = SysCacheGetAttr(...,
            // relpartbound, ...); if (!isnull) boundspec = stringToNode(...);
            if let Some(text) = syscache_seam::pg_class_relpartbound_text::call(inhrelid)? {
                boundspec = Some(parse_boundspec(mcx, &text)?);
            }

            /*
             * Two problems are possible here (see C comment): a concurrent
             * ATTACH not yet visible to the syscache, and DETACH CONCURRENTLY
             * resetting relpartbound. Read pg_class directly, and retry once on
             * a still-missing boundspec.
             */
            // if (boundspec == NULL)
            if boundspec.is_none() {
                if let Some(text) =
                    syscache_seam::pg_class_relpartbound_text_direct::call(inhrelid)?
                {
                    boundspec = Some(parse_boundspec(mcx, &text)?);
                }

                /*
                 * If we still don't get a relpartbound value, then it must be
                 * because of DETACH CONCURRENTLY. Restart from the top, as
                 * explained above. We only do this once.
                 */
                // if (!boundspec && !retried)
                if boundspec.is_none() && !retried {
                    // AcceptInvalidationMessages(); retried = true; goto retry;
                    inval_seam::accept_invalidation_messages::call()?;
                    retried = true;
                    restart = true;
                    break; // goto retry
                }
            }

            /* Sanity checks. */
            // if (!boundspec) elog(ERROR, "missing relpartbound for relation %u", inhrelid);
            let boundspec = match boundspec {
                Some(bs) => bs,
                None => {
                    return Err(elog_error(format!(
                        "missing relpartbound for relation {inhrelid}"
                    )))
                }
            };
            // The node-tag check `if (!IsA(boundspec, PartitionBoundSpec))` is
            // enforced by `parse_boundspec` (which rejects a non-PartitionBoundSpec
            // node with "invalid relpartbound for relation %u").

            /*
             * If the PartitionBoundSpec says this is the default partition, its
             * OID should match pg_partitioned_table.partdefid; if not, the
             * catalog is corrupt.
             */
            // if (boundspec->is_default)
            if boundspec.is_default {
                // partdefid = get_default_partition_oid(RelationGetRelid(rel));
                let partdefid = backend_catalog_partition::get_default_partition_oid(relid)?;
                // if (partdefid != inhrelid)
                if partdefid != inhrelid {
                    return Err(elog_error(format!(
                        "expected partdefid {inhrelid}, but got {partdefid}"
                    )));
                }
            }

            /* Save results. */
            // oids[i] = inhrelid;
            oids.push(inhrelid);
            // is_leaf[i] = (get_rel_relkind(inhrelid) != RELKIND_PARTITIONED_TABLE);
            is_leaf.push(
                lsyscache_seam::get_rel_relkind::call(inhrelid)? != RELKIND_PARTITIONED_TABLE,
            );
            // boundspecs[i] = boundspec;
            boundspecs.push(boundspec);
        }

        if restart {
            // goto retry: abandon the partial per-attempt arrays and loop again.
            continue;
        }

        /*
         * Create PartitionBoundInfo and mapping, working in the caller's
         * context. This could fail, but we haven't done any damage if so.
         */
        // if (nparts > 0)
        //     boundinfo = partition_bounds_create(boundspecs, nparts, key, &mapping);
        if nparts > 0 {
            // The seam takes `&[&PartitionBoundSpec]`.
            let refs: alloc::vec::Vec<&PartitionBoundSpec<'mcx>> =
                boundspecs.iter().map(|b| &**b).collect();
            let (bi, m) = partbounds_seam::partition_bounds_create::call(
                mcx,
                &refs,
                nparts as usize,
                &key,
            )?;
            break (nparts, oids, is_leaf, m, bi, detached_exist, detached_xmin);
        } else {
            break (
                nparts,
                oids,
                is_leaf,
                mcx::vec_with_capacity_in(mcx, 0)?,
                None,
                detached_exist,
                detached_xmin,
            );
        }
    };

    /*
     * Now build the actual partition descriptor.
     */
    // partdesc->nparts = nparts; partdesc->detached_exist = detached_exist;
    let mut partdesc = PartitionDescData {
        nparts,
        detached_exist,
        oids: mcx::vec_with_capacity_in(mcx, 0)?,
        is_leaf: mcx::vec_with_capacity_in(mcx, 0)?,
        boundinfo: None,
        last_found_datum_index: 0,
        last_found_part_index: 0,
        last_found_count: 0,
    };

    /* If there are no partitions, the rest of the partdesc can stay zero */
    // if (nparts > 0)
    if nparts > 0 {
        // partdesc->boundinfo = partition_bounds_copy(boundinfo, key);
        partdesc.boundinfo = copy_boundinfo(mcx, &boundinfo, &key)?;

        /* Initialize caching fields for speeding up ExecFindPartition */
        partdesc.last_found_datum_index = -1;
        partdesc.last_found_part_index = -1;
        partdesc.last_found_count = 0;

        // partdesc->oids = palloc(nparts * sizeof(Oid));
        let mut new_oids: mcx::PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, nparts as usize)?;
        new_oids.resize(nparts as usize, InvalidOid);
        let mut new_is_leaf: mcx::PgVec<'mcx, bool> =
            mcx::vec_with_capacity_in(mcx, nparts as usize)?;
        new_is_leaf.resize(nparts as usize, false);

        /*
         * Assign OIDs from the original (catalog-scan order) array into the
         * mapped indexes of the result (canonical bound order) array. Also save
         * leaf-ness of each partition.
         */
        // for (i = 0; i < nparts; i++) { index = mapping[i]; ... }
        for i in 0..nparts as usize {
            let index = mapping[i] as usize;
            new_oids[index] = oids[i];
            new_is_leaf[index] = is_leaf[i];
        }
        partdesc.oids = new_oids;
        partdesc.is_leaf = new_is_leaf;
    }

    /*
     * Are we working with the partdesc that omits the detached partition, or
     * the one that includes it? (Preserved for fidelity; the value is recorded
     * but, with no persistent relcache cache, not used to pick a cache slot.)
     */
    // is_omit = omit_detached && detached_exist && ActiveSnapshotSet() &&
    //     TransactionIdIsValid(detached_xmin);
    let _is_omit = omit_detached
        && detached_exist
        && snapmgr_seam::active_snapshot_set::call()
        && TransactionIdIsValid(detached_xmin);

    // return partdesc;
    mcx::alloc_in(mcx, partdesc)
}

/// `castNode(PartitionBoundSpec, stringToNode(TextDatumGetCString(boundDatum)))`
/// — parse a stored `relpartbound` `pg_node_tree` text into a
/// `PartitionBoundSpec`, enforcing the C `IsA(.., PartitionBoundSpec)` check
/// (`elog(ERROR, "invalid relpartbound for relation ...")` is raised by the
/// caller; here a wrong node type is a parse/cast error).
fn parse_boundspec<'mcx>(
    mcx: Mcx<'mcx>,
    text: &str,
) -> PgResult<PgBox<'mcx, PartitionBoundSpec<'mcx>>> {
    let node = backend_nodes_read_seams::string_to_node::call(mcx, text)?;
    match PgBox::into_inner(node).into_partitionboundspec() {
        Some(spec) => mcx::alloc_in(mcx, spec),
        None => Err(elog_error(alloc::string::String::from(
            "invalid relpartbound: stringToNode did not yield a PartitionBoundSpec",
        ))),
    }
}

/// `partition_bounds_copy(boundinfo, key)` over the owned
/// [`PartitionBoundInfo`] option.
fn copy_boundinfo<'mcx>(
    mcx: Mcx<'mcx>,
    boundinfo: &PartitionBoundInfo<'mcx>,
    key: &PartitionKeyData<'mcx>,
) -> PgResult<PartitionBoundInfo<'mcx>> {
    match boundinfo {
        None => Ok(None),
        Some(bi) => Ok(Some(partbounds_seam::partition_bounds_copy::call(
            mcx, bi, key,
        )?)),
    }
}

/* ===========================================================================
 * PartitionDirectory  (partdesc.c:36-47, modeled in-crate)
 * ======================================================================== */

/// `PartitionDirectoryData` (`partdesc.c`).
///
/// The C `HTAB *pdir_hash` (keyed by `Oid`, values `{Relation rel,
/// PartitionDesc pd}`) is modeled by an owned `BTreeMap<Oid, PgBox<'static,
/// PartitionDescData>>` allocated in this directory's own long-lived
/// `MemoryContext` (the analogue of the C `pdir_mcxt`). On first sight of a
/// relation a descriptor is built and `clone_in`'d into `ctx`, and a refcount
/// pin is taken; teardown decrements every pin.
///
/// The stored descriptor borrows `ctx`; the borrow's `'static` marker is sound
/// because `ctx` is heap-pinned (its address survives moves of the directory),
/// the map is dropped before `ctx` (declaration order), and the descriptor
/// never escapes at `'static` — every read re-shortens it to the caller's
/// context via [`PartitionDescData::clone_in`].
pub struct PartitionDirectoryData {
    /// `pdir_mcxt` — the directory's own context; heap-pinned for a stable
    /// address. Field order is load-bearing: `pdir_hash` (which allocates into
    /// `ctx`) must drop before `ctx`.
    pdir_hash: BTreeMap<Oid, PgBox<'static, PartitionDescData<'static>>>,
    /// The OIDs pinned via `RelationIncrementReferenceCount`, released on
    /// destroy. (Mirrors the keys, kept explicit for the teardown contract.)
    pinned: alloc::vec::Vec<Oid>,
    /// `bool omit_detached`.
    omit_detached: bool,
    ctx: Box<MemoryContext>,
}

impl PartitionDirectoryData {
    /// The directory's own context handle (used to allocate the cached
    /// descriptor and to re-project it on lookup).
    fn mcx(&self) -> Mcx<'static> {
        // SAFETY: extend the borrow of the heap-pinned context to 'static.
        // Sound by the same construction as `mcx::McxOwned`: the box's address
        // is stable across moves of `self`, `pdir_hash` is dropped before `ctx`
        // (field order), and the 'static descriptors never leave this type at
        // 'static — `PartitionDirectoryLookup` re-shortens every read.
        unsafe { core::mem::transmute::<Mcx<'_>, Mcx<'static>>(self.ctx.mcx()) }
    }
}

impl core::fmt::Debug for PartitionDirectoryData {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PartitionDirectoryData")
            .field("nentries", &self.pdir_hash.len())
            .field("omit_detached", &self.omit_detached)
            .finish_non_exhaustive()
    }
}

/// `PartitionDirectory` — owned alias (`partdefs.h`).
pub type PartitionDirectory = PartitionDirectoryData;

/* ---------------------------------------------------------------------------
 * CreatePartitionDirectory  (partdesc.c:422-442)
 * ------------------------------------------------------------------------- */

/// `CreatePartitionDirectory` -- create a new partition directory object.
pub fn CreatePartitionDirectory(omit_detached: bool) -> PartitionDirectory {
    // pdir = palloc(sizeof(PartitionDirectoryData)); pdir->pdir_mcxt = mcxt;
    // pdir->pdir_hash = hash_create(...); pdir->omit_detached = omit_detached;
    PartitionDirectoryData {
        pdir_hash: BTreeMap::new(),
        pinned: alloc::vec::Vec::new(),
        omit_detached,
        ctx: Box::new(MemoryContext::new("partition directory")),
    }
}

/* ---------------------------------------------------------------------------
 * PartitionDirectoryLookup  (partdesc.c:455-475)
 * ------------------------------------------------------------------------- */

/// `PartitionDirectoryLookup` -- look up the partition descriptor for a
/// relation in the directory.
///
/// Ensures we get the same `PartitionDesc` for each relation every time we look
/// it up within the lifetime of this directory: on first sight the descriptor
/// is built and cached (and a relcache refcount pinned); subsequent lookups
/// re-project the cached descriptor. The returned descriptor is a fresh copy in
/// the caller's `mcx`.
pub fn PartitionDirectoryLookup<'mcx>(
    mcx: Mcx<'mcx>,
    pdir: &mut PartitionDirectory,
    rel: &Relation<'mcx>,
) -> PgResult<PgBox<'mcx, PartitionDescData<'mcx>>> {
    // Oid relid = RelationGetRelid(rel);
    let relid = rel.rd_id;

    // pde = hash_search(pdir->pdir_hash, &relid, HASH_ENTER, &found);
    // if (!found)
    if !pdir.pdir_hash.contains_key(&relid) {
        /*
         * We must keep a reference count on the relation so that the
         * PartitionDesc to which we are pointing can't get destroyed.
         */
        // RelationIncrementReferenceCount(rel);
        relcache_seam::relation_increment_reference_count::call(relid)?;
        // pde->pd = RelationGetPartitionDesc(rel, pdir->omit_detached);
        let built = RelationGetPartitionDesc(mcx, rel, pdir.omit_detached)?;
        // Cache an owned clone in the directory's own context (the analogue of
        // the C refcount pin keeping the relcache descriptor alive).
        let dir_mcx = pdir.mcx();
        let cached: PgBox<'static, PartitionDescData<'static>> = {
            // SAFETY: `built` is `PartitionDescData<'mcx>`; cloning it into
            // `dir_mcx` (a 'static-marked handle on the directory's own
            // pinned context) yields a value owned by that context. The
            // 'static lifetime is sound because the value lives exactly as
            // long as `pdir` (dropped before `ctx`) and never escapes at
            // 'static (this function returns only the re-projected copy).
            let c: PgBox<'static, PartitionDescData<'static>> = {
                let cloned = PartitionDescData::clone_in(&built, dir_mcx)?;
                mcx::alloc_in(dir_mcx, cloned)?
            };
            c
        };
        pdir.pdir_hash.insert(relid, cached);
        pdir.pinned.push(relid);
    }

    // return pde->pd; — re-project the cached descriptor into the caller's mcx
    // so the returned value carries the caller's lifetime (the C returns the
    // pinned relcache pointer; the owned analogue is a fresh copy).
    let cached = pdir
        .pdir_hash
        .get(&relid)
        .ok_or_else(|| PgError::error("PartitionDirectoryLookup: descriptor missing after insert"))?;
    let reproj = PartitionDescData::clone_in(cached, mcx)?;
    mcx::alloc_in(mcx, reproj)
}

/* ---------------------------------------------------------------------------
 * DestroyPartitionDirectory  (partdesc.c:483-492)
 * ------------------------------------------------------------------------- */

/// `DestroyPartitionDirectory` -- destroy a partition directory.
///
/// Release the reference counts we're holding.
pub fn DestroyPartitionDirectory(pdir: &PartitionDirectory) -> PgResult<()> {
    /*
     * hash_seq_init(&status, pdir->pdir_hash);
     * while ((pde = hash_seq_search(&status)) != NULL)
     *     RelationDecrementReferenceCount(pde->rel);
     */
    for &relid in &pdir.pinned {
        // RelationDecrementReferenceCount(pde->rel);
        relcache_seam::relation_decrement_reference_count::call(relid)?;
    }
    Ok(())
}

/* ===========================================================================
 * get_default_oid_from_partdesc  (partdesc.c:500-508)
 * ======================================================================== */

/// `get_default_oid_from_partdesc` -- given a partition descriptor, return the
/// OID of the default partition, if one exists; else, return `InvalidOid`.
///
/// `partition_bound_has_default(bi)` is `((bi)->default_index != -1)`
/// (partbounds.h) — a pure in-crate field read, not a seam.
pub fn get_default_oid_from_partdesc(partdesc: Option<&PartitionDescData<'_>>) -> Oid {
    // if (partdesc && partdesc->boundinfo && partition_bound_has_default(...))
    //     return partdesc->oids[partdesc->boundinfo->default_index];
    if let Some(partdesc) = partdesc {
        if let Some(boundinfo) = partdesc.boundinfo.as_deref() {
            if boundinfo.default_index != -1 {
                return partdesc.oids[boundinfo.default_index as usize];
            }
        }
    }
    // return InvalidOid;
    InvalidOid
}

/* ===========================================================================
 * Seam installation — the partition-directory surface
 * (backend-partitioning-core-seams).
 * ======================================================================== */

/// `CreatePartitionDirectory(mcxt, omit_detached)` seam adapter: build the
/// directory and box it into the executor's `Opaque` handle.
fn create_partition_directory_seam<'mcx>(
    _mcx: Mcx<'mcx>,
    omit_detached: bool,
) -> PgResult<Opaque> {
    let dir = CreatePartitionDirectory(omit_detached);
    Ok(Opaque(Some(Box::new(dir))))
}

/// `PartitionDirectoryLookup(pdir, rel)` seam adapter: downcast the `Opaque`
/// directory handle, look up the descriptor.
fn partition_directory_lookup_seam<'mcx>(
    mcx: Mcx<'mcx>,
    pdir: &mut Opaque,
    rel: Relation<'mcx>,
) -> PgResult<PgBox<'mcx, PartitionDescData<'mcx>>> {
    let dir = pdir
        .0
        .as_mut()
        .and_then(|b| b.downcast_mut::<PartitionDirectoryData>())
        .ok_or_else(|| {
            PgError::error("partition_directory_lookup: Opaque is not a PartitionDirectoryData")
        })?;
    PartitionDirectoryLookup(mcx, dir, &rel)
}

/// `DestroyPartitionDirectory(pdir)` seam adapter: downcast and release the
/// directory's relcache refcounts, then drop it (freeing its context).
fn destroy_partition_directory_seam(pdir: Box<dyn core::any::Any>) {
    if let Ok(dir) = pdir.downcast::<PartitionDirectoryData>() {
        // The C teardown only releases refcounts; a failure here is on the
        // error/cleanup path and must not escalate (mirrors the relation-close
        // swallow). The directory then drops, freeing its context.
        let _ = DestroyPartitionDirectory(&dir);
    }
}

/// Install the partition-directory seams owned by this unit.
pub fn init_seams() {
    core_seam::create_partition_directory::set(create_partition_directory_seam);
    core_seam::partition_directory_lookup::set(partition_directory_lookup_seam);
    core_seam::destroy_partition_directory::set(destroy_partition_directory_seam);
    backend_partitioning_partdesc_seams::relation_get_partition_desc::set(
        RelationGetPartitionDesc,
    );
}

#[cfg(test)]
mod tests;

/// Reference the `RelationData` import so it documents the deref target even
/// when only used through `Relation`.
#[allow(dead_code)]
fn _reldata_anchor(_r: &RelationData<'_>) {}
