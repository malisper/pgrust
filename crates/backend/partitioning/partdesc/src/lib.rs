// partdesc.c. C divergence: descriptors cached in partdesc-owned maps keyed
// by relid (same relcache inval as C's rd_partdesc / rd_partdesc_nodetached).
#![allow(non_snake_case)]

use core::cell::{Cell, RefCell};
use core::mem::ManuallyDrop;
use std::rc::Rc;

use datum::Datum;
use mcx::{Mcx, MemoryContext, PgHashMap, PgVec};
use types_core::{InvalidOid, InvalidTransactionId, Oid, TransactionId};
use types_error::PgResult;
use types_nodes::rawnodes::PartitionBoundSpec;
use types_nodes::NodeList;
use types_rel::{Relation, RELKIND_PARTITIONED_TABLE};

use partbounds::PartitionBoundInfoData;

const RELOID: i32 = cache_syscache::cacheinfo::RELOID;
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
    if let Some(d) = with_state(|st| st.descs.get(&relid).map(Rc::clone)) {
        if !d.detached_exist || !omit_detached {
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

// text varlena -> &str; long bound lists arrive pglz-compressed inline.
fn text_to_str<'mcx>(mcx: ::mcx::Mcx<'mcx>, d: Datum) -> &'mcx str {
    let p = d.as_usize() as *const u8;
    // SAFETY: syscache text attribute; toasted/compressed images are loud.
    unsafe {
        let b0 = *p;
        let (len, off) = if b0 & 0x01 != 0 {
            if b0 == 0x01 {
                panic!("partdesc: toasted relpartbound unported");
            }
            ((((b0 as usize) >> 1) & 0x7F) - 1, 1)
        } else {
            let w = u32::from_ne_bytes(core::slice::from_raw_parts(p, 4).try_into().unwrap());
            if w & 0x02 != 0 {
                let total = ::types_tuple::varatt::varsize_any(p);
                let raw = core::slice::from_raw_parts(p, total);
                let flat = ::detoast_seams::detoast_attr::call(mcx, raw)
                    .expect("detoast relpartbound");
                let (ptr, len) = (flat.as_ptr(), flat.len());
                core::mem::forget(flat);
                // detoast_attr returns the full 4-byte-header image; the
                // payload follows. Arena-backed until mcx reset; forget only
                // skips the vec's own dealloc.
                let s = core::slice::from_raw_parts(ptr.add(4), len - 4);
                return core::str::from_utf8(s).expect("non-UTF-8 relpartbound");
            }
            ((w as usize >> 2) - 4, 4)
        };
        core::str::from_utf8(core::slice::from_raw_parts(p.add(off), len))
            .expect("non-UTF-8 relpartbound")
    }
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

    let mut oids: PgVec<'_, Oid> = mcx::vec_with_capacity_in(smcx, nparts)?;
    let mut is_leaf: PgVec<'_, bool> = mcx::vec_with_capacity_in(smcx, nparts)?;
    let mut boundspecs: Vec<&PartitionBoundSpec<'_>> = Vec::with_capacity(nparts);

    for &inhrelid in inhoids.iter() {
        let tuple = cache_syscache::SearchSysCache1(
            RELOID,
            cache_syscache::SysCacheKey::Value(Datum::from_oid(inhrelid)),
        )?
        .unwrap_or_else(|| panic!("cache lookup failed for relation {inhrelid}"));
        let (datum, isnull) =
            cache_syscache::SysCacheGetAttr(RELOID, &tuple, Anum_pg_class_relpartbound)?;
        if isnull {
            panic!("missing relpartbound for relation {inhrelid}");
        }
        let node = readfuncs::stringToNode(smcx, text_to_str(smcx, datum))?;
        cache_syscache::ReleaseSysCache(tuple);
        let spec = node
            .as_variant::<PartitionBoundSpec>()
            .unwrap_or_else(|| panic!("invalid relpartbound for relation {inhrelid}"));
        boundspecs.push(spec);
        oids.push(inhrelid);
        is_leaf.push(lsyscache::get_rel_relkind(inhrelid)? != RELKIND_PARTITIONED_TABLE as i8);
    }

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
