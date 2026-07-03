// partdesc.c: RelationGetPartitionDesc / RelationBuildPartitionDesc.
// C divergences: descriptors cached in a partdesc-owned map keyed by relid
// (cleared by the same relcache inval that clears C's rd_partdesc); the
// DETACH CONCURRENTLY omit/retry protocol is unported (loud in pg_inherits).
#![allow(non_snake_case)]

use core::cell::{Cell, RefCell};
use core::mem::ManuallyDrop;
use std::rc::Rc;

use datum::Datum;
use mcx::{Mcx, MemoryContext, PgHashMap, PgVec};
use types_core::{InvalidOid, Oid};
use types_error::PgResult;
use types_nodes::rawnodes::PartitionBoundSpec;
use types_rel::{Relation, RELKIND_PARTITIONED_TABLE};

use partbounds::PartitionBoundInfoData;

const RELOID: i32 = cache_syscache::cacheinfo::RELOID;
const Anum_pg_class_relpartbound: i32 = 34;

pub struct PartitionDescData {
    pub nparts: usize,
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
    callbacks_registered: bool,
}

thread_local! {
    static STATE: RefCell<Option<ManuallyDrop<PartDescState>>> = const { RefCell::new(None) };
}

fn with_state<R>(f: impl FnOnce(&mut PartDescState) -> R) -> R {
    STATE.with(|cell| {
        let mut slot = cell.borrow_mut();
        let st = slot.get_or_insert_with(|| {
            let mcx = Box::leak(Box::new(MemoryContext::new("PartDescContext"))).mcx();
            ManuallyDrop::new(PartDescState {
                mcx,
                descs: PgHashMap::with_capacity_in(8, mcx),
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
        } else {
            st.descs.clear();
        }
    });
}

pub fn RelationGetPartitionDesc(rel: &Relation<'_>) -> PgResult<Rc<PartitionDescData>> {
    debug_assert!(rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE);
    let relid = rel.rd_id;
    if let Some(d) = with_state(|st| st.descs.get(&relid).map(Rc::clone)) {
        return Ok(d);
    }
    RelationBuildPartitionDesc(rel)
}

// text varlena -> &str, inline images only (relpartbound is written inline).
fn text_to_str(d: Datum) -> &'static str {
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
                panic!("partdesc: compressed relpartbound unported");
            }
            ((w as usize >> 2) - 4, 4)
        };
        core::str::from_utf8(core::slice::from_raw_parts(p.add(off), len))
            .expect("non-UTF-8 relpartbound")
    }
}

#[inline(never)]
fn RelationBuildPartitionDesc(rel: &Relation<'_>) -> PgResult<Rc<PartitionDescData>> {
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

    let inhoids = pg_inherits::find_inheritance_children(smcx, relid, types_rel::NoLock)?;
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
        let node = readfuncs::stringToNode(smcx, text_to_str(datum))?;
        cache_syscache::ReleaseSysCache(tuple);
        let spec = node
            .as_variant::<PartitionBoundSpec>()
            .unwrap_or_else(|| panic!("invalid relpartbound for relation {inhrelid}"));
        if spec.is_default {
            panic!("partdesc: default partitions unported (relation {inhrelid})");
        }
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
            oids: PgVec::new_in(cmcx),
            is_leaf: PgVec::new_in(cmcx),
            boundinfo: None,
            last_found_datum_index: Cell::new(-1),
            last_found_part_index: Cell::new(-1),
            last_found_count: Cell::new(0),
        }
    };

    let desc = Rc::new(desc);
    with_state(|st| st.descs.insert(relid, Rc::clone(&desc)));
    Ok(desc)
}
