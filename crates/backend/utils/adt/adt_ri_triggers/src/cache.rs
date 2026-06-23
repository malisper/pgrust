//! The process-local RI caches and their accessors.
//!
//! `ri_triggers.c` keeps three private dynahash tables that live as long as the
//! backend: `ri_constraint_cache` (the [`RiConstraintInfo`] per FK constraint),
//! `ri_query_cache` (saved `SPIPlanPtr` per [`RiQueryKey`]) and
//! `ri_compare_cache` (the cmp/cast function OIDs per `RiCompareKey`). These are
//! PROCESS-LOCAL (no shared memory), modelled as `thread_local!` per-backend
//! tables. C caches resolved `FmgrInfo`s in the compare cache; an `FmgrInfo`
//! embeds a C function pointer and cannot cross a seam, so we cache the
//! resolved function OIDs and dispatch by OID through the fmgr seams at call
//! time (`ri_hash_compare_op` / `ri_compare_with_cast`).

extern crate alloc;
use alloc::string::String;
use core::cell::RefCell;
use std::collections::HashMap;

use ::mcx::Mcx;
use ::types_core::primitive::OidIsValid;
use ::types_core::{InvalidOid, Oid};
use ::types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_OBJECT_DEFINITION,
};
use ::types_ri_triggers::{SpiPlanPtr, TriggerRef};

use ::coerce_seams::CoercionPathType;

use crate::{
    name_data_from_bytes, RelSide, RiConstraintInfo, RiQueryKey, FKCONSTR_MATCH_FULL,
    FKCONSTR_MATCH_PARTIAL, FKCONSTR_MATCH_SIMPLE, RI_MAX_NUMKEYS, RI_PLAN_CHECK_LOOKUPPK_FROM_PK,
};

type QueryCacheEntry = Option<SpiPlanPtr>;

/// `RI_CompareKey` --- the key identifying an entry showing how to compare two
/// values.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct RiCompareKey {
    /// `Oid eq_opr` --- the equality operator to apply.
    pub eq_opr: Oid,
    /// `Oid typeid` --- the data type to apply it to.
    pub typeid: Oid,
}

/// `RI_CompareHashEntry`.
///
/// C caches the resolved `FmgrInfo`s; an `FmgrInfo` embeds a C function pointer
/// and cannot cross a seam, so the owned model caches the resolved function
/// OIDs and dispatches by OID through the fmgr seams at call time.
#[derive(Clone, Copy)]
pub struct RiCompareHashEntry {
    /// `eq_opr_finfo` --- the equality fn (the operator's `oprcode`).
    pub eq_opr_func: Oid,
    /// `cast_func_finfo` --- the input-coercion fn, or `InvalidOid` if none.
    pub cast_func: Oid,
}

thread_local! {
    static RI_CONSTRAINT_CACHE: RefCell<Option<HashMap<Oid, RiConstraintInfo>>> =
        const { RefCell::new(None) };
    static RI_QUERY_CACHE: RefCell<Option<HashMap<RiQueryKey, QueryCacheEntry>>> =
        const { RefCell::new(None) };
    static RI_COMPARE_CACHE: RefCell<Option<HashMap<RiCompareKey, RiCompareHashEntry>>> =
        const { RefCell::new(None) };
    static RI_CONSTRAINT_VALID_LIST: RefCell<alloc::vec::Vec<Oid>> =
        const { RefCell::new(alloc::vec::Vec::new()) };
}

/// `ri_InitHashTables` --- initialize our internal hash tables and register the
/// `InvalidateConstraintCacheCallBack` on `CONSTROID`.
pub fn ri_init_hash_tables() -> PgResult<()> {
    RI_CONSTRAINT_CACHE.with(|c| {
        if c.borrow().is_none() {
            *c.borrow_mut() = Some(HashMap::new());
        }
    });
    pg_constraint_seams::register_constraint_inval_callback::call()?;
    RI_QUERY_CACHE.with(|c| {
        if c.borrow().is_none() {
            *c.borrow_mut() = Some(HashMap::new());
        }
    });
    RI_COMPARE_CACHE.with(|c| {
        if c.borrow().is_none() {
            *c.borrow_mut() = Some(HashMap::new());
        }
    });
    Ok(())
}

/// `ri_HashCompareOp` --- see if we know how to compare two values, and create
/// a new hash entry if not.
pub fn ri_hash_compare_op(mcx: Mcx<'_>, eq_opr: Oid, typeid: Oid) -> PgResult<RiCompareHashEntry> {
    // On the first call initialize the hashtable.
    let initialized = RI_COMPARE_CACHE.with(|c| c.borrow().is_some());
    if !initialized {
        ri_init_hash_tables()?;
    }

    let key = RiCompareKey { eq_opr, typeid };

    // Find or create a hash entry.
    let found = RI_COMPARE_CACHE.with(|c| c.borrow().as_ref().unwrap().get(&key).copied());
    if let Some(entry) = found {
        return Ok(entry);
    }

    // Not already initialized: do so. (C keeps the entry for the life of the
    // backend; here the resolved FmgrInfos are re-resolved by OID at call time,
    // so the entry just records the function OIDs.)
    //
    // We always need to know how to call the equality operator. `get_opcode`
    // resolves the operator's `oprcode`; `fmgr_info_check` validates it the way
    // C's `fmgr_info_cxt` would.
    let eq_opr_func = lsyscache_seams::get_opcode::call(eq_opr)?;
    fmgr_seams::fmgr_info_check::call(eq_opr_func)?;

    // If we chose to use a cast from FK to PK type, we may have to apply the
    // cast function to get to the operator's input type.
    let (lefttype, _righttype) = lsyscache_seams::op_input_types::call(eq_opr)?;
    let castfunc = if typeid == lefttype {
        InvalidOid // simplest case
    } else {
        let (pathtype, castfunc) =
            ::coerce_seams::find_coercion_pathway_implicit::call(lefttype, typeid)?;
        if pathtype != CoercionPathType::Func && pathtype != CoercionPathType::Relabeltype {
            // The declared input type of the eq_opr might be a polymorphic type
            // such as ANYARRAY or ANYENUM, or other special cases such as
            // RECORD; find_coercion_pathway currently doesn't subsume these
            // special cases.
            if !::coerce_seams::is_binary_coercible::call(typeid, lefttype)? {
                let from = format_type_seams::format_type_be::call(mcx, typeid)?;
                let to = format_type_seams::format_type_be::call(mcx, lefttype)?;
                return Err(PgError::error(alloc::format!(
                    "no conversion function from {} to {}",
                    from.as_str(),
                    to.as_str()
                )));
            }
        }
        castfunc
    };
    let cast_func = if OidIsValid(castfunc) {
        fmgr_seams::fmgr_info_check::call(castfunc)?;
        castfunc
    } else {
        InvalidOid
    };

    let entry = RiCompareHashEntry {
        eq_opr_func,
        cast_func,
    };
    RI_COMPARE_CACHE.with(|c| {
        c.borrow_mut().as_mut().unwrap().insert(key, entry);
    });
    Ok(entry)
}

/// `ri_FetchConstraintInfo` --- fetch the [`RiConstraintInfo`] for the trigger's
/// FK constraint, with the easy cross-checks against the trigger call data.
pub fn ri_fetch_constraint_info(
    mcx: Mcx<'_>,
    trigger: TriggerRef,
    trig_rel: RelSide<'_, '_>,
    rel_is_pk: bool,
) -> PgResult<RiConstraintInfo> {
    let constraint_oid = trigger_seams::trigger_constraint::call(trigger);

    if constraint_oid == InvalidOid {
        let tgname = String::from_utf8_lossy(
            &trigger_seams::trigger_name::call(mcx, trigger)?,
        )
        .into_owned();
        let relname = String::from_utf8_lossy(&trig_rel.name_bytes(mcx)?).into_owned();
        return Err(PgError::error(alloc::format!(
            "no pg_constraint entry for trigger \"{tgname}\" on table \"{relname}\""
        ))
        .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION)
        .with_hint(
            "Remove this referential integrity trigger and its mates, then do ALTER TABLE ADD CONSTRAINT.",
        ));
    }

    let riinfo = ri_load_constraint_info(mcx, constraint_oid)?;

    let tgconstrrelid = trigger_seams::trigger_constrrelid::call(trigger);
    let trig_relid = trig_rel.oid();
    if rel_is_pk {
        if riinfo.fk_relid != tgconstrrelid || riinfo.pk_relid != trig_relid {
            let tgname = String::from_utf8_lossy(
                &trigger_seams::trigger_name::call(mcx, trigger)?,
            )
            .into_owned();
            let relname = String::from_utf8_lossy(&trig_rel.name_bytes(mcx)?).into_owned();
            return Err(PgError::error(alloc::format!(
                "wrong pg_constraint entry for trigger \"{tgname}\" on table \"{relname}\""
            )));
        }
    } else if riinfo.fk_relid != trig_relid || riinfo.pk_relid != tgconstrrelid {
        let tgname = String::from_utf8_lossy(
            &trigger_seams::trigger_name::call(mcx, trigger)?,
        )
        .into_owned();
        let relname = String::from_utf8_lossy(&trig_rel.name_bytes(mcx)?).into_owned();
        return Err(PgError::error(alloc::format!(
            "wrong pg_constraint entry for trigger \"{tgname}\" on table \"{relname}\""
        )));
    }

    if riinfo.confmatchtype != FKCONSTR_MATCH_FULL
        && riinfo.confmatchtype != FKCONSTR_MATCH_PARTIAL
        && riinfo.confmatchtype != FKCONSTR_MATCH_SIMPLE
    {
        return Err(PgError::error(alloc::format!(
            "unrecognized confmatchtype: {}",
            riinfo.confmatchtype
        )));
    }

    if riinfo.confmatchtype == FKCONSTR_MATCH_PARTIAL {
        return Err(PgError::error("MATCH PARTIAL not yet implemented")
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    Ok(riinfo)
}

/// `ri_LoadConstraintInfo` --- fetch or create the [`RiConstraintInfo`] for an
/// FK constraint (returns a `Copy` of the cached entry).
pub fn ri_load_constraint_info(mcx: Mcx<'_>, constraint_oid: Oid) -> PgResult<RiConstraintInfo> {
    let inited = RI_CONSTRAINT_CACHE.with(|c| c.borrow().is_some());
    if !inited {
        ri_init_hash_tables()?;
    }

    let cached = RI_CONSTRAINT_CACHE.with(|c| {
        let mut b = c.borrow_mut();
        let map = b.as_mut().expect("constraint cache inited");
        let entry = map
            .entry(constraint_oid)
            .or_insert_with(|| RiConstraintInfo::new(constraint_oid));
        if entry.valid {
            Some(*entry)
        } else {
            None
        }
    });
    if let Some(riinfo) = cached {
        return Ok(riinfo);
    }

    let row = match pg_constraint_seams::load_fk_constraint::call(
        mcx,
        constraint_oid,
    )? {
        Some(row) => row,
        None => {
            return Err(PgError::error(alloc::format!(
                "cache lookup failed for constraint {constraint_oid}"
            )));
        }
    };

    let mut riinfo = RiConstraintInfo::new(constraint_oid);

    if row.conparentid != InvalidOid {
        riinfo.constraint_root_id =
            pg_constraint_seams::get_ri_constraint_root::call(row.conparentid)?;
    } else {
        riinfo.constraint_root_id = constraint_oid;
    }
    riinfo.oid_hash_value = row.oid_hash_value;
    riinfo.root_hash_value =
        pg_constraint_seams::constraint_hash_value::call(riinfo.constraint_root_id)?;
    riinfo.conname = name_data_from_bytes(&row.conname);
    riinfo.pk_relid = row.pk_relid;
    riinfo.fk_relid = row.fk_relid;
    riinfo.confupdtype = row.confupdtype;
    riinfo.confdeltype = row.confdeltype;
    riinfo.confmatchtype = row.confmatchtype;
    riinfo.hasperiod = row.hasperiod;

    riinfo.nkeys = row.nkeys;
    copy_into(&mut riinfo.fk_attnums, &row.fk_attnums);
    copy_into(&mut riinfo.pk_attnums, &row.pk_attnums);
    copy_into(&mut riinfo.pf_eq_oprs, &row.pf_eq_oprs);
    copy_into(&mut riinfo.pp_eq_oprs, &row.pp_eq_oprs);
    copy_into(&mut riinfo.ff_eq_oprs, &row.ff_eq_oprs);
    riinfo.ndelsetcols = row.ndelsetcols;
    copy_into(&mut riinfo.confdelsetcols, &row.confdelsetcols);

    if riinfo.hasperiod {
        // C (ri_triggers.c:2340): opclass = get_index_column_opclass(conindid, nkeys).
        let opclass = lsyscache_seams::get_index_column_opclass::call(
            row.conindid,
            riinfo.nkeys,
        )?;
        let opers = pg_constraint_seams::find_fk_period_opers::call(opclass)?;
        riinfo.period_contained_by_oper = opers.period_contained_by_oper;
        riinfo.agged_period_contained_by_oper = opers.agged_period_contained_by_oper;
        riinfo.period_intersect_oper = opers.period_intersect_oper;
    }

    RI_CONSTRAINT_VALID_LIST.with(|l| {
        let mut list = l.borrow_mut();
        list.try_reserve(1).map_err(|_| mcx.oom(core::mem::size_of::<Oid>()))?;
        list.push(constraint_oid);
        Ok::<(), PgError>(())
    })?;

    riinfo.valid = true;

    RI_CONSTRAINT_CACHE.with(|c| {
        let mut b = c.borrow_mut();
        let map = b.as_mut().expect("constraint cache inited");
        map.insert(constraint_oid, riinfo);
    });

    Ok(riinfo)
}

/// `InvalidateConstraintCacheCallBack` --- invalidate matching cache entries.
pub fn invalidate_constraint_cache_callback(hashvalue: u32) {
    let inited = RI_CONSTRAINT_CACHE.with(|c| c.borrow().is_some());
    if !inited {
        return;
    }

    let mut hashvalue = hashvalue;
    let count = RI_CONSTRAINT_VALID_LIST.with(|l| l.borrow().len());
    if count > 1000 {
        hashvalue = 0;
    }

    RI_CONSTRAINT_VALID_LIST.with(|l| {
        let mut list = l.borrow_mut();
        list.retain(|&oid| {
            let matched = RI_CONSTRAINT_CACHE.with(|c| {
                let mut b = c.borrow_mut();
                let map = b.as_mut().expect("constraint cache inited");
                if let Some(riinfo) = map.get_mut(&oid) {
                    if hashvalue == 0
                        || riinfo.oid_hash_value == hashvalue
                        || riinfo.root_hash_value == hashvalue
                    {
                        riinfo.valid = false;
                        return true;
                    }
                }
                false
            });
            !matched
        });
    });
}

/// `ri_FetchPreparedPlan` --- look up a query key; return the plan if found AND
/// still valid, else `None`.
pub fn ri_fetch_prepared_plan(key: &RiQueryKey) -> PgResult<Option<SpiPlanPtr>> {
    let inited = RI_QUERY_CACHE.with(|c| c.borrow().is_some());
    if !inited {
        ri_init_hash_tables()?;
    }

    let plan = RI_QUERY_CACHE.with(|c| {
        let b = c.borrow();
        let map = b.as_ref().expect("query cache inited");
        map.get(key).copied().flatten()
    });
    let plan = match plan {
        Some(plan) => plan,
        None => return Ok(None),
    };

    if spi_seams::spi_plan_is_valid::call(plan) {
        return Ok(Some(plan));
    }

    RI_QUERY_CACHE.with(|c| {
        let mut b = c.borrow_mut();
        let map = b.as_mut().expect("query cache inited");
        map.insert(*key, None);
    });
    spi_seams::spi_freeplan::call(plan)?;

    Ok(None)
}

/// `ri_HashPreparedPlan` --- add a plan to our private SPI plan hashtable.
pub fn ri_hash_prepared_plan(key: &RiQueryKey, plan: SpiPlanPtr) -> PgResult<()> {
    let inited = RI_QUERY_CACHE.with(|c| c.borrow().is_some());
    if !inited {
        ri_init_hash_tables()?;
    }

    RI_QUERY_CACHE.with(|c| {
        let mut b = c.borrow_mut();
        let map = b.as_mut().expect("query cache inited");
        debug_assert!(
            !matches!(map.get(key), Some(Some(_))),
            "Assert(!found || entry->plan == NULL)"
        );
        map.insert(*key, Some(plan));
    });
    Ok(())
}

/// `ri_BuildQueryKey` --- construct a hashtable key for a prepared SPI plan.
pub fn ri_build_query_key(riinfo: &RiConstraintInfo, constr_queryno: i32) -> RiQueryKey {
    let constr_id = if constr_queryno != RI_PLAN_CHECK_LOOKUPPK_FROM_PK {
        riinfo.constraint_root_id
    } else {
        riinfo.constraint_id
    };
    RiQueryKey {
        constr_id,
        constr_queryno,
    }
}

fn copy_into<T: Copy>(dst: &mut [T; RI_MAX_NUMKEYS], src: &[T]) {
    let n = src.len().min(RI_MAX_NUMKEYS);
    dst[..n].copy_from_slice(&src[..n]);
}
