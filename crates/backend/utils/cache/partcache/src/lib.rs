// partcache.c: RelationBuildPartitionKey / RelationGetPartitionKey.
// C divergence: keys are cached in a partcache-owned map keyed by relid
// (invalidated by the same relcache event that clears C's rd_partkey) rather
// than inside the relcache entry.
#![allow(non_snake_case)]

use core::cell::RefCell;
use core::mem::ManuallyDrop;
use std::rc::Rc;

use datum::Datum;
use mcx::{Mcx, MemoryContext, PgHashMap, PgVec};
use types_core::{AttrNumber, InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_INVALID_OBJECT_DEFINITION, ERROR};
use types_fmgr::{FmgrInfo, LocalFcinfo};
use types_rel::Relation;

pub const PARTITION_STRATEGY_LIST: i8 = b'l' as i8;
pub const PARTITION_STRATEGY_RANGE: i8 = b'r' as i8;
pub const PARTITION_STRATEGY_HASH: i8 = b'h' as i8;
pub const PARTITION_MAX_KEYS: usize = 32;

const PARTRELID: i32 = cache_syscache::cacheinfo::PARTRELID;
const CLAOID: i32 = cache_syscache::cacheinfo::CLAOID;
const BTORDER_PROC: i16 = 1;

const Anum_pg_partitioned_table_partstrat: i32 = 2;
const Anum_pg_partitioned_table_partnatts: i32 = 3;
const Anum_pg_partitioned_table_partdefid: i32 = 4;
const Anum_pg_partitioned_table_partattrs: i32 = 5;
const Anum_pg_partitioned_table_partclass: i32 = 6;
const Anum_pg_partitioned_table_partcollation: i32 = 7;
const Anum_pg_partitioned_table_partexprs: i32 = 8;

pub struct PartitionKeyData {
    pub strategy: i8,
    pub partnatts: i16,
    pub partattrs: PgVec<'static, AttrNumber>,
    pub partopfamily: PgVec<'static, Oid>,
    pub partopcintype: PgVec<'static, Oid>,
    // std Vec justified: Rc-owned owner structure outside the arenas;
    // FmgrInfo is droppy (rd_supportinfo precedent). RefCell: invoke() takes
    // &mut; DDL-only here — per-row routing clones into its dispatch carrier.
    pub partsupfunc: Vec<RefCell<FmgrInfo>>,
    pub partcollation: PgVec<'static, Oid>,
    pub parttypid: PgVec<'static, Oid>,
    pub parttypmod: PgVec<'static, i32>,
    pub parttyplen: PgVec<'static, i16>,
    pub parttypbyval: PgVec<'static, bool>,
    pub parttypalign: PgVec<'static, i8>,
    pub parttypcoll: PgVec<'static, Oid>,
}

impl PartitionKeyData {
    // FunctionCall2Coll(&partsupfunc[col], partcollation[col], a, b) -> int32.
    pub fn cmp(&self, col: usize, a: Datum, b: Datum) -> PgResult<i32> {
        let mut fcinfo = LocalFcinfo::<2>::new(self.partcollation[col]);
        fcinfo.set_arg(0, a);
        fcinfo.set_arg(1, b);
        let mut f = self.partsupfunc[col].borrow_mut();
        let r = f.invoke(&mut fcinfo)?;
        if fcinfo.isnull {
            panic!("partition support function {} returned NULL", f.fn_oid);
        }
        Ok(r.as_i32())
    }
}

struct PartCacheState {
    mcx: Mcx<'static>,
    keys: PgHashMap<'static, Oid, Rc<PartitionKeyData>>,
    callbacks_registered: bool,
}

thread_local! {
    static STATE: RefCell<Option<ManuallyDrop<PartCacheState>>> = const { RefCell::new(None) };
}

fn with_state<R>(f: impl FnOnce(&mut PartCacheState) -> R) -> R {
    STATE.with(|cell| {
        let mut slot = cell.borrow_mut();
        let st = slot.get_or_insert_with(|| {
            let mcx = Box::leak(Box::new(MemoryContext::new("PartCacheContext"))).mcx();
            ManuallyDrop::new(PartCacheState {
                mcx,
                keys: PgHashMap::with_capacity_in(8, mcx),
                callbacks_registered: false,
            })
        });
        f(st)
    })
}

fn PartCacheRelCallback(_arg: Datum, relid: Oid) {
    with_state(|st| {
        if relid != InvalidOid {
            st.keys.remove(&relid);
        } else {
            st.keys.clear();
        }
    });
}

// Vector varlena image (int2vector/oidvector): 24B header, values at 24.
fn vector_values(d: Datum, elmlen: usize) -> (usize, *const u8) {
    let p = d.as_usize() as *const u8;
    // SAFETY: pg_partitioned_table vector columns are inline 4B-header images.
    unsafe {
        let vl = u32::from_ne_bytes(core::slice::from_raw_parts(p, 4).try_into().unwrap());
        debug_assert_eq!(vl & 0x03, 0);
        let dim = i32::from_ne_bytes(core::slice::from_raw_parts(p.add(16), 4).try_into().unwrap());
        let _ = elmlen;
        (dim as usize, p.add(24))
    }
}

pub fn RelationGetPartitionKey(rel: &Relation<'_>) -> PgResult<Rc<PartitionKeyData>> {
    let relid = rel.rd_id;
    if let Some(k) = with_state(|st| st.keys.get(&relid).map(Rc::clone)) {
        return Ok(k);
    }
    RelationBuildPartitionKey(rel)
}

#[inline(never)]
fn RelationBuildPartitionKey(rel: &Relation<'_>) -> PgResult<Rc<PartitionKeyData>> {
    let relid = rel.rd_id;
    if !with_state(|st| st.callbacks_registered) {
        inval::invalidate::CacheRegisterRelcacheCallback(
            PartCacheRelCallback,
            Datum::from_oid(InvalidOid),
        )?;
        with_state(|st| st.callbacks_registered = true);
    }

    let tuple = cache_syscache::SearchSysCache1(
        PARTRELID,
        cache_syscache::SysCacheKey::Value(Datum::from_oid(relid)),
    )?
    .unwrap_or_else(|| panic!("cache lookup failed for partition key of relation {relid}"));

    let mcx = with_state(|st| st.mcx);
    let (strategy, partnatts);
    let mut partattrs: PgVec<'static, AttrNumber>;
    let mut partclass: PgVec<'static, Oid> = PgVec::new_in(mcx);
    let mut partcollation: PgVec<'static, Oid> = PgVec::new_in(mcx);
    {
        strategy = cache_syscache::SysCacheGetAttrNotNull(
            PARTRELID,
            &tuple,
            Anum_pg_partitioned_table_partstrat,
        )?
        .as_i8();
        partnatts = cache_syscache::SysCacheGetAttrNotNull(
            PARTRELID,
            &tuple,
            Anum_pg_partitioned_table_partnatts,
        )?
        .as_i16();
        let n = partnatts as usize;
        let (attrs_d, _) = cache_syscache::SysCacheGetAttr(
            PARTRELID,
            &tuple,
            Anum_pg_partitioned_table_partattrs,
        )?;
        let (nattrs, ap) = vector_values(attrs_d, 2);
        assert_eq!(nattrs, n);
        partattrs = mcx::vec_with_capacity_in(mcx, n)?;
        for i in 0..n {
            // SAFETY: int2vector carries n aligned i16 values at data start.
            partattrs.push(unsafe {
                i16::from_ne_bytes(
                    core::slice::from_raw_parts(ap.add(2 * i), 2).try_into().unwrap(),
                )
            });
        }
        let class_d = cache_syscache::SysCacheGetAttrNotNull(
            PARTRELID,
            &tuple,
            Anum_pg_partitioned_table_partclass,
        )?;
        let (ncls, cp) = vector_values(class_d, 4);
        assert_eq!(ncls, n);
        let coll_d = cache_syscache::SysCacheGetAttrNotNull(
            PARTRELID,
            &tuple,
            Anum_pg_partitioned_table_partcollation,
        )?;
        let (ncoll, colp) = vector_values(coll_d, 4);
        assert_eq!(ncoll, n);
        partclass.reserve(n);
        partcollation.reserve(n);
        for i in 0..n {
            // SAFETY: oidvector carries n aligned u32 values at data start.
            unsafe {
                partclass.push(u32::from_ne_bytes(
                    core::slice::from_raw_parts(cp.add(4 * i), 4).try_into().unwrap(),
                ));
                partcollation.push(u32::from_ne_bytes(
                    core::slice::from_raw_parts(colp.add(4 * i), 4).try_into().unwrap(),
                ));
            }
        }
        let (_, exprs_null) = cache_syscache::SysCacheGetAttr(
            PARTRELID,
            &tuple,
            Anum_pg_partitioned_table_partexprs,
        )?;
        if !exprs_null {
            panic!("partcache: expression partition keys unported (relation {relid})");
        }
    }
    cache_syscache::ReleaseSysCache(tuple);

    if strategy != PARTITION_STRATEGY_LIST && strategy != PARTITION_STRATEGY_RANGE {
        if strategy == PARTITION_STRATEGY_HASH {
            panic!("partcache: HASH partition keys unported (relation {relid})");
        }
        panic!("invalid partition strategy \"{}\"", strategy as u8 as char);
    }

    let n = partnatts as usize;
    let mut key = PartitionKeyData {
        strategy,
        partnatts,
        partattrs,
        partopfamily: mcx::vec_with_capacity_in(mcx, n)?,
        partopcintype: mcx::vec_with_capacity_in(mcx, n)?,
        partsupfunc: Vec::with_capacity(n),
        partcollation,
        parttypid: mcx::vec_with_capacity_in(mcx, n)?,
        parttypmod: mcx::vec_with_capacity_in(mcx, n)?,
        parttyplen: mcx::vec_with_capacity_in(mcx, n)?,
        parttypbyval: mcx::vec_with_capacity_in(mcx, n)?,
        parttypalign: mcx::vec_with_capacity_in(mcx, n)?,
        parttypcoll: mcx::vec_with_capacity_in(mcx, n)?,
    };

    for i in 0..n {
        let opclasstup = cache_syscache::SearchSysCache1(
            CLAOID,
            cache_syscache::SysCacheKey::Value(Datum::from_oid(partclass[i])),
        )?
        .unwrap_or_else(|| panic!("cache lookup failed for opclass {}", partclass[i]));
        // pg_opclass: opcfamily attnum 6, opcintype attnum 7.
        let opcfamily =
            cache_syscache::SysCacheGetAttrNotNull(CLAOID, &opclasstup, 6)?.as_oid();
        let opcintype =
            cache_syscache::SysCacheGetAttrNotNull(CLAOID, &opclasstup, 7)?.as_oid();
        cache_syscache::ReleaseSysCache(opclasstup);
        key.partopfamily.push(opcfamily);
        key.partopcintype.push(opcintype);

        let funcid = lsyscache::get_opfamily_proc(opcfamily, opcintype, opcintype, BTORDER_PROC)?;
        if funcid == InvalidOid {
            return Err(missing_support_function(partclass[i]));
        }
        key.partsupfunc.push(RefCell::new(
            fmgr_seams::fmgr_info::call(funcid)
                .unwrap_or_else(|e| panic!("fmgr_info({funcid}) failed: {e:?}")),
        ));

        let attno = key.partattrs[i];
        assert!(attno != 0, "expression partition keys unported");
        let att = rel.descr().attr(attno as usize - 1);
        key.parttypid.push(att.atttypid);
        key.parttypmod.push(att.atttypmod);
        key.parttypcoll.push(att.attcollation);
        let (typlen, typbyval, typalign) = lsyscache::get_typlenbyvalalign(att.atttypid)?;
        key.parttyplen.push(typlen);
        key.parttypbyval.push(typbyval);
        key.parttypalign.push(typalign);
    }

    let key = Rc::new(key);
    with_state(|st| st.keys.insert(relid, Rc::clone(&key)));
    Ok(key)
}

#[cold]
#[inline(never)]
fn missing_support_function(opclass: Oid) -> Box<PgError> {
    Box::new(
        PgError::new(
            ERROR,
            format!(
                "operator class {opclass} of access method btree is missing support function 1"
            ),
        )
        .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
    )
}

pub fn get_default_partition_oid(parent_relid: Oid) -> PgResult<Oid> {
    let Some(tuple) = cache_syscache::SearchSysCache1(
        PARTRELID,
        cache_syscache::SysCacheKey::Value(Datum::from_oid(parent_relid)),
    )?
    else {
        return Ok(InvalidOid);
    };
    let defid = cache_syscache::SysCacheGetAttrNotNull(
        PARTRELID,
        &tuple,
        Anum_pg_partitioned_table_partdefid,
    )?
    .as_oid();
    cache_syscache::ReleaseSysCache(tuple);
    Ok(defid)
}
