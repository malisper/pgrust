// partdesc.c. C divergence: descriptors cached in partdesc-owned maps keyed
// by relid (same relcache inval as C's rd_partdesc / rd_partdesc_nodetached).
#![allow(non_snake_case)]

use core::cell::{Cell, RefCell};
use core::mem::ManuallyDrop;
use std::rc::Rc;

use datum::Datum;
use mcx::{Mcx, MemoryContext, PgHashMap, PgVec};
use types_core::{InvalidOid, InvalidTransactionId, Oid, TransactionId};
use types_error::{PgError, PgResult};
use types_core::AttrNumber;
use types_nodes::rawnodes::PartitionBoundSpec;
use types_nodes::{Node, NodeList};
use types_rel::{Relation, RELKIND_PARTITIONED_TABLE};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

use partbounds::PartitionBoundInfoData;

const RELOID: i32 = cache_syscache::cacheinfo::RELOID;
#[allow(non_upper_case_globals)] // C-parity name
const Anum_pg_class_oid: i32 = 1;
#[allow(non_upper_case_globals)] // C-parity name
const Anum_pg_class_relpartbound: i32 = 34;

pub struct PartitionDescData {
    pub nparts: usize,
    pub detached_exist: bool,
    pub oids: PgVec<'static, Oid>,
    pub is_leaf: PgVec<'static, bool>,
    pub boundinfo: Option<PartitionBoundInfoData<'static>>,
    // C's last-found routing cache (rule-5; get_partition_for_tuple).
    pub last_found_datum_index: Cell<i32>,
    pub last_found_part_index: Cell<i32>,
    pub last_found_count: Cell<i32>,
}

struct PartDescState {
    mcx: Mcx<'static>,
    descs: PgHashMap<'static, Oid, Rc<PartitionDescData>>,
    descs_nodetached: PgHashMap<'static, Oid, (Rc<PartitionDescData>, TransactionId)>,
    // C rd_partcheck: cached partition constraint per partition relid.
    quals: PgHashMap<'static, Oid, NodeList<'static>>,
    callbacks_registered: bool,
}

thread_local! {
    static STATE: RefCell<Option<ManuallyDrop<PartDescState>>> = const { RefCell::new(None) };
}

fn with_state<R>(f: impl FnOnce(&mut PartDescState) -> R) -> R {
    STATE.with(|cell| {
        let mut slot = cell.borrow_mut();
        let st = slot.get_or_insert_with(|| {
            let mcx = ::mcx::session_root("PartDescContext").mcx();
            ManuallyDrop::new(PartDescState {
                mcx,
                descs: PgHashMap::with_capacity_in(8, mcx),
                descs_nodetached: PgHashMap::with_capacity_in(8, mcx),
                quals: PgHashMap::with_capacity_in(8, mcx),
                callbacks_registered: false,
            })
        });
        f(st)
    })
}

fn PartDescRelCallback(_arg: Datum, relid: Oid) {
    with_state(|st| {
        if relid != InvalidOid {
            st.descs.remove(&relid);
            st.descs_nodetached.remove(&relid);
            st.quals.remove(&relid);
        } else {
            st.descs.clear();
            st.descs_nodetached.clear();
            st.quals.clear();
        }
    });
}

pub fn RelationGetPartitionDesc(
    rel: &Relation<'_>,
    omit_detached: bool,
) -> PgResult<Rc<PartitionDescData>> {
    debug_assert!(rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE);
    let relid = rel.rd_id;
    // partdesc.c:83-86: with no active snapshot detached partitions are not
    // omitted either, so the cached descriptor serves that case too.
    if let Some(d) = with_state(|st| st.descs.get(&relid).map(Rc::clone)) {
        if !d.detached_exist || !omit_detached || !snapmgr::ActiveSnapshotSet() {
            return Ok(d);
        }
    }
    if omit_detached && snapmgr::ActiveSnapshotSet() {
        if let Some((d, xmin)) =
            with_state(|st| st.descs_nodetached.get(&relid).map(|(d, x)| (Rc::clone(d), *x)))
        {
            debug_assert!(xmin != InvalidTransactionId);
            let snap = snapmgr::GetActiveSnapshot();
            if !snapmgr::XidInMVCCSnapshot(xmin, &snap)? {
                return Ok(d);
            }
        }
    }
    RelationBuildPartitionDesc(rel, omit_detached)
}

// text varlena -> &str; long bound lists arrive pglz-compressed inline or as
// external TOAST pointers into pg_class's TOAST table (C TextDatumGetCString
// detoasts either form via pg_detoast_datum_packed).  C hands the bytes to
// stringToNode unvalidated (partdesc.c:198) and a corrupted image ends in
// one of its elogs; our reader takes &str, so an image that is not UTF-8
// is reported as partdesc.c:281's "invalid relpartbound" elog for the
// partition rather than a backend abort.
fn text_to_str<'mcx>(mcx: ::mcx::Mcx<'mcx>, d: Datum, inhrelid: Oid) -> PgResult<&'mcx str> {
    let p = d.as_usize() as *const u8;
    // SAFETY: catalog text attribute; header forms dispatched as C VARATT_IS_*.
    let bytes: &'mcx [u8] = unsafe {
        let b0 = *p;
        if b0 & 0x01 != 0 {
            if b0 == 0x01 {
                // External TOAST pointer (C VARATT_IS_EXTERNAL).
                detoast_text_bytes(mcx, p)?
            } else {
                core::slice::from_raw_parts(p.add(1), (((b0 as usize) >> 1) & 0x7F) - 1)
            }
        } else {
            let w = u32::from_ne_bytes(core::slice::from_raw_parts(p, 4).try_into().unwrap());
            if w & 0x02 != 0 {
                detoast_text_bytes(mcx, p)?
            } else {
                core::slice::from_raw_parts(p.add(4), (w as usize >> 2) - 4)
            }
        }
    };
    core::str::from_utf8(bytes).map_err(|_| invalid_relpartbound(inhrelid))
}

// External or compressed relpartbound image -> flat payload bytes via
// detoast_attr (a detoast failure is its own elog, as in C).
//
// # Safety
// `p` points to a live varlena image (toast pointer or compressed 4B form).
unsafe fn detoast_text_bytes<'mcx>(mcx: ::mcx::Mcx<'mcx>, p: *const u8) -> PgResult<&'mcx [u8]> {
    let total = ::types_tuple::varatt::varsize_any(p);
    let raw = core::slice::from_raw_parts(p, total);
    let flat = ::detoast_seams::detoast_attr::call(mcx, raw)?;
    let (ptr, len) = (flat.as_ptr(), flat.len());
    core::mem::forget(flat);
    // detoast_attr returns the full 4-byte-header image; the payload follows.
    // Arena-backed until mcx reset; forget only skips the vec's own dealloc.
    Ok(core::slice::from_raw_parts(ptr.add(4), len - 4))
}

// partdesc.c:279 elog(ERROR, "missing relpartbound for relation %u") -- the
// terminal report once the syscache probe, the direct pg_class scan and the
// single retry have all failed to produce a bound.  elog's default SQLSTATE
// is XX000 and it unwinds the transaction; it never aborts the backend.
#[cold]
#[inline(never)]
fn missing_relpartbound(inhrelid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "missing relpartbound for relation {inhrelid}"
    )))
}

// partdesc.c:281 elog(ERROR, "invalid relpartbound for relation %u").
#[cold]
#[inline(never)]
fn invalid_relpartbound(inhrelid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "invalid relpartbound for relation {inhrelid}"
    )))
}

// partdesc.c:295 elog(ERROR, "expected partdefid %u, but got %u"): the
// bound says DEFAULT but pg_partitioned_table.partdefid names another
// relation -- a corrupt catalog, reported rather than routed on.
#[cold]
#[inline(never)]
fn expected_partdefid(inhrelid: Oid, partdefid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "expected partdefid {inhrelid}, but got {partdefid}"
    )))
}

// partdesc.c:225-256: the syscache could not supply a relpartbound (a
// concurrent ATTACH PARTITION may have committed after the catcache last
// saw the row, or DETACH CONCURRENTLY may be mid-way); read pg_class
// directly for the tuple.  None when the row is gone (dropped meanwhile)
// or its relpartbound is NULL.
fn relpartbound_from_pg_class<'mcx>(
    mcx: Mcx<'mcx>,
    inhrelid: Oid,
) -> PgResult<Option<Node<'mcx>>> {
    let pg_class = table::table_open(
        mcx,
        types_core::catalog::RELATION_RELATION_ID,
        types_rel::AccessShareLock,
    )?;
    // C ScanKeyInit(Anum_pg_class_oid, BTEqualStrategyNumber, F_OIDEQ, oid).
    let mut key = ScanKeyData::empty();
    key.sk_attno = Anum_pg_class_oid as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = InvalidOid;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)?;
    key.sk_argument = Datum::from_oid(inhrelid);
    let mut scan =
        genam::systable_beginscan(mcx, &pg_class, catalog::ClassOidIndexId, true, None, &[key])?;
    let mut boundspec = None;
    // One tuple in the normal case, none if the table was dropped meanwhile.
    if let Some(tuple) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY: a pg_class tuple read under pg_class's own descriptor.
        let datum = unsafe {
            ::types_tuple::heap_getattr(
                tuple,
                Anum_pg_class_relpartbound,
                pg_class.descr(),
                &mut isnull,
            )
        };
        if !isnull {
            boundspec = Some(readfuncs::stringToNode(mcx, text_to_str(mcx, datum, inhrelid)?)?);
        }
    }
    genam::systable_endscan(mcx, scan)?;
    pg_class.close(types_rel::AccessShareLock)?;
    Ok(boundspec)
}

#[inline(never)]
fn RelationBuildPartitionDesc(
    rel: &Relation<'_>,
    omit_detached: bool,
) -> PgResult<Rc<PartitionDescData>> {
    let relid = rel.rd_id;
    if !with_state(|st| st.callbacks_registered) {
        inval::invalidate::CacheRegisterRelcacheCallback(
            PartDescRelCallback,
            Datum::from_oid(InvalidOid),
        )?;
        with_state(|st| st.callbacks_registered = true);
    }

    // Parse-lifetime scratch for the relpartbound trees.
    let scratch = MemoryContext::new("partition descriptor scratch");
    let smcx = scratch.mcx();

    // partdesc.c:134-276: the child list and every bound are read together;
    // a bound the syscache cannot supply is re-read from pg_class directly,
    // and if that still yields nothing the whole walk restarts once after
    // AcceptInvalidationMessages() (DETACH CONCURRENTLY resets relpartbound
    // in a second transaction after marking the pg_inherits row pending, so
    // the list and the bounds can otherwise disagree).  Once only: a single
    // DETACH CONCURRENTLY can affect us at a time, and a corrupt catalog
    // must not loop forever.
    let mut retried = false;
    let (inhoids, detached_exist, detached_xmin, boundspecs, is_leaf) = 'retry: loop {
        let mut detached_exist = false;
        let mut detached_xmin = InvalidTransactionId;
        let inhoids = pg_inherits::find_inheritance_children_extended(
            smcx,
            relid,
            omit_detached,
            types_rel::NoLock,
            Some(&mut detached_exist),
            Some(&mut detached_xmin),
        )?;
        let nparts = inhoids.len();
        let mut is_leaf: PgVec<'_, bool> = mcx::vec_with_capacity_in(smcx, nparts)?;
        let mut boundspecs: Vec<&PartitionBoundSpec<'_>> = Vec::with_capacity(nparts);

        for &inhrelid in inhoids.iter() {
            // Try fetching the tuple from the catcache, for speed.
            let mut boundspec: Option<Node<'_>> = None;
            if let Some(tuple) = cache_syscache::SearchSysCache1(
                RELOID,
                cache_syscache::SysCacheKey::Value(Datum::from_oid(inhrelid)),
            )? {
                let (datum, isnull) =
                    cache_syscache::SysCacheGetAttr(RELOID, &tuple, Anum_pg_class_relpartbound)?;
                if !isnull {
                    boundspec =
                        Some(readfuncs::stringToNode(smcx, text_to_str(smcx, datum, inhrelid)?)?);
                }
                cache_syscache::ReleaseSysCache(tuple);
            }
            if boundspec.is_none() {
                boundspec = relpartbound_from_pg_class(smcx, inhrelid)?;
                if boundspec.is_none() && !retried {
                    inval::local::AcceptInvalidationMessages()?;
                    retried = true;
                    continue 'retry;
                }
            }

            // Sanity checks (partdesc.c:278-281).
            let Some(node) = boundspec else {
                return Err(missing_relpartbound(inhrelid));
            };
            let Some(spec) = node.as_variant::<PartitionBoundSpec>() else {
                return Err(invalid_relpartbound(inhrelid));
            };

            // partdesc.c:289-297: a DEFAULT bound must be the partition that
            // pg_partitioned_table.partdefid names.
            if spec.is_default {
                let partdefid = partcache::get_default_partition_oid(relid)?;
                if partdefid != inhrelid {
                    return Err(expected_partdefid(inhrelid, partdefid));
                }
            }

            boundspecs.push(spec);
            is_leaf.push(lsyscache::get_rel_relkind(inhrelid)? != RELKIND_PARTITIONED_TABLE as i8);
        }
        break (inhoids, detached_exist, detached_xmin, boundspecs, is_leaf);
    };
    let nparts = inhoids.len();
    let oids = inhoids;

    let cmcx = with_state(|st| st.mcx);
    let desc = if nparts > 0 {
        let key = partcache::RelationGetPartitionKey(rel)?;
        let (boundinfo, mapping) = partbounds::partition_bounds_create(cmcx, &boundspecs, &key)?;
        let mut mapped_oids: PgVec<'static, Oid> = mcx::vec_with_capacity_in(cmcx, nparts)?;
        let mut mapped_leaf: PgVec<'static, bool> = mcx::vec_with_capacity_in(cmcx, nparts)?;
        mapped_oids.resize(nparts, InvalidOid);
        mapped_leaf.resize(nparts, false);
        for i in 0..nparts {
            let index = mapping[i] as usize;
            mapped_oids[index] = oids[i];
            mapped_leaf[index] = is_leaf[i];
        }
        PartitionDescData {
            nparts,
            detached_exist,
            oids: mapped_oids,
            is_leaf: mapped_leaf,
            boundinfo: Some(boundinfo),
            last_found_datum_index: Cell::new(-1),
            last_found_part_index: Cell::new(-1),
            last_found_count: Cell::new(0),
        }
    } else {
        PartitionDescData {
            nparts: 0,
            detached_exist,
            oids: PgVec::new_in(cmcx),
            is_leaf: PgVec::new_in(cmcx),
            boundinfo: None,
            last_found_datum_index: Cell::new(-1),
            last_found_part_index: Cell::new(-1),
            last_found_count: Cell::new(0),
        }
    };

    let desc = Rc::new(desc);
    // Snapshot-dependent (a pending row omitted by xmin visibility) => only
    // the nodetached slot, keyed by that xmin (partdesc.c:363-402).
    if omit_detached && detached_exist && detached_xmin != InvalidTransactionId {
        with_state(|st| st.descs_nodetached.insert(relid, (Rc::clone(&desc), detached_xmin)));
    } else {
        with_state(|st| st.descs.insert(relid, Rc::clone(&desc)));
    }
    Ok(desc)
}

// RelationGetPartitionQual + generate_partition_qual (partcache.c), hosted
// here for partdesc access (partcache -> partbounds would cycle); cached per
// relid under the same relcache invalidation as the descriptors.
pub fn RelationGetPartitionQual<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
) -> PgResult<NodeList<'mcx>> {
    if !rel.rd_rel.relispartition {
        return Ok(NodeList::nil());
    }
    let q = generate_partition_qual(rel)?;
    // SAFETY: the cached qual lives in the leaked (never-freed)
    // PartDescContext, so shortening 'static to 'mcx only narrows the view.
    let q = unsafe { core::mem::transmute::<NodeList<'static>, NodeList<'mcx>>(q) };
    // C copyObject at every exit (partcache.c:352-353, 420): callers scribble
    // varnos in place (plancat's ChangeVarNodes); a shallow clone lets that
    // corrupt the cache, and map_partition_varattnos then skips the
    // non-varno-1 ancestor Vars of every descendant's qual generated later.
    rewrite_manip::copy_node_list(mcx, &q)
}

fn generate_partition_qual<'mcx>(rel: &Relation<'mcx>) -> PgResult<NodeList<'static>> {
    // C partcache.c:349: recurses up the partition parent chain.
    stack_depth_core::check_stack_depth()?;
    let relid = rel.rd_id;
    let cmcx0 = with_state(|st| st.mcx);
    if let Some(q) = with_state(|st| st.quals.get(&relid).map(|q| q.clone_in(cmcx0))) {
        return q;
    }
    if !with_state(|st| st.callbacks_registered) {
        inval::invalidate::CacheRegisterRelcacheCallback(
            PartDescRelCallback,
            Datum::from_oid(InvalidOid),
        )?;
        with_state(|st| st.callbacks_registered = true);
    }
    let cmcx = with_state(|st| st.mcx);
    let parent_oid = pg_inherits::get_partition_parent(cmcx, relid, true)?;
    // C relation_open (index partitions reach here too); their relpartbound
    // is NULL and their parent is a partitioned index with no partition key.
    let parent = relation_seams::relation_open::call(cmcx, parent_oid, types_rel::AccessShareLock)?;
    let my_qual = match partbounds::read_boundspec_opt(cmcx, relid)? {
        Some(spec) => {
            let key = partcache::RelationGetPartitionKey(&parent)?;
            let pdesc = RelationGetPartitionDesc(&parent, false)?;
            partbounds::get_qual_from_partbound(
                cmcx,
                &key,
                parent_oid,
                pdesc.boundinfo.as_ref(),
                &pdesc.oids,
                spec,
            )?
        }
        None => NodeList::nil(),
    };
    let mut result = NodeList::nil();
    if parent.rd_rel.relispartition {
        for q in generate_partition_qual(&parent)?.iter() {
            result.lappend(cmcx, q)?;
        }
    }
    for q in my_qual.iter() {
        result.lappend(cmcx, q)?;
    }
    let result = partbounds::map_partition_varattnos(cmcx, result, 1, rel, &parent)?;
    parent.close(types_rel::NoLock)?;
    let out = result.clone_in(cmcx)?;
    with_state(|st| st.quals.insert(relid, result));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mock owner of the detoast seam for this test binary (see the partbounds
    // twin): expects the 18-byte ondisk toast pointer built below and hands
    // back the flat 4B-header text image.
    fn mock_detoast<'mcx>(
        mcx: ::mcx::Mcx<'mcx>,
        image: &[u8],
    ) -> PgResult<::mcx::PgVec<'mcx, u8>> {
        assert_eq!(image[0], 0x01, "external toast pointer tag byte");
        assert_eq!(image[1], ::types_tuple::varatt::VARTAG_ONDISK);
        assert_eq!(image.len(), 18, "VARHDRSZ_EXTERNAL + vartag_size(ONDISK)");
        let payload = b"{LIST (a, b, c)}";
        let total = ::types_tuple::varatt::VARHDRSZ + payload.len();
        let mut v = ::mcx::vec_with_capacity_in(mcx, total)?;
        v.extend_from_slice(
            &::types_tuple::varatt::set_varsize_4b_word(total as u32).to_ne_bytes(),
        );
        v.extend_from_slice(payload);
        Ok(v)
    }

    // Witness for the retired "toasted relpartbound unported" fence: an
    // externally-toasted relpartbound must be fetched, as C's
    // RelationBuildPartitionDesc does via TextDatumGetCString (partdesc.c).
    #[test]
    fn external_relpartbound_is_detoasted() {
        ::detoast_seams::detoast_attr::set(mock_detoast);
        let cx = MemoryContext::new("partdesc detoast test");
        // varattrib_1b_e ondisk image: [0x01, VARTAG_ONDISK, 16 payload bytes].
        let mut image = [0u8; 18];
        image[0] = 0x01;
        image[1] = ::types_tuple::varatt::VARTAG_ONDISK;
        let s = text_to_str(cx.mcx(), Datum::from_usize(image.as_ptr() as usize), 1).unwrap();
        assert_eq!(s, "{LIST (a, b, c)}");
    }

    // audit-18.6 b094: C's RelationBuildPartitionDesc hands the relpartbound
    // bytes to stringToNode unvalidated (partdesc.c:198 TextDatumGetCString);
    // whatever a corrupted catalog holds ends in an elog(ERROR), never in a
    // backend abort.  A non-UTF-8 image must therefore not panic here.
    #[test]
    fn non_utf8_relpartbound_does_not_panic() {
        let cx = MemoryContext::new("partdesc non-utf8 test");
        // 1B short header: (payload + header byte) << 1 | 1, then two bytes
        // that are not valid UTF-8 in any position.
        let image: [u8; 3] = [(3u8 << 1) | 0x01, 0xff, 0xfe];
        let d = Datum::from_usize(image.as_ptr() as usize);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            text_to_str(cx.mcx(), d, 16396).map(|_| ())
        }));
        let Ok(result) = outcome else {
            panic!("non-UTF-8 relpartbound must not panic");
        };
        // partdesc.c:281's elog for the partition: catchable XX000.
        let err = result.expect_err("non-UTF-8 relpartbound must be an error");
        assert_eq!(err.message(), "invalid relpartbound for relation 16396");
        assert_eq!(err.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
        assert_eq!(err.level(), types_error::ERROR);
    }
}
