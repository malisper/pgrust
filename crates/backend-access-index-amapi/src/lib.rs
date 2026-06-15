//! Port of `src/backend/access/index/amapi.c` — support routines for the index
//! access-method API.
//!
//! `GetIndexAmRoutine(amhandler)` calls the AM's handler function (in C an
//! `OidFunctionCall0(amhandler)` returning an `IndexAmRoutine*`) to obtain its
//! [`IndexAmRoutine`] vtable. As the C comment on `GetIndexAmRoutine` notes,
//! built-in handlers involve no catalog access (relcache.c relies on this for
//! bootstrapping the system-catalog indexes), so the dispatch is a direct match
//! on the well-known handler-function OIDs to the built-in AM handlers
//! (`bthandler` / `hashhandler`), exactly the set the fmgr builtin table would
//! reach. AMs added by extensions would be reached through their dynamically
//! loaded handler; that path seam-and-panics until a dynamic-fmgr leg lands
//! (`mirror PG and panic`).
//!
//! `GetIndexAmRoutineByAmId(amoid)` resolves the AM's handler OID through the
//! `pg_am` syscache (the `search_am_handler` projection) and then calls
//! `GetIndexAmRoutine`. The `amtype != AMTYPE_INDEX` rejection the C performs is
//! folded into that index-AM-specific lookup (the projection is documented as
//! "look up the index AM's handler").
//!
//! `IndexAmTranslateStrategy` / `IndexAmTranslateCompareType` translate between
//! AM-specific strategy numbers and the AM-independent [`CompareType`], with the
//! btree fast paths the C keeps.
//!
//! `init_seams()` installs the caller-shaped projections the
//! `backend-access-index-amapi-seams` crate declares — the `IndexAmRoutine`
//! vtable / flag readers across the optimizer, catalog, relcache, and logical
//! replication that all bottom out in one of the routines above.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use types_core::primitive::Oid;
use types_error::{
    PgError, PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
};
use types_opclass::IndexAmInfo;
use types_scan::scankey::{InvalidStrategy, StrategyNumber};
use types_tableam::amapi::{
    CompareType, IndexAmRoutine, COMPARE_GT, COMPARE_INVALID,
};

use backend_access_index_amapi_seams as sx;
use backend_utils_cache_syscache_seams as syscache;

/// `BTREE_AM_OID` (catalog/pg_am.dat) — the built-in btree access method.
const BTREE_AM_OID: Oid = 403;

/// `F_BTHANDLER` (pg_proc.dat oid 330) — the btree AM handler function.
const F_BTHANDLER: Oid = 330;
/// `F_HASHHANDLER` (pg_proc.dat oid 331) — the hash AM handler function.
const F_HASHHANDLER: Oid = 331;

/// `BTMaxStrategyNumber` (access/nbtree.h) — the btree fast-path bound used by
/// `IndexAmTranslateStrategy`.
const BTMaxStrategyNumber: StrategyNumber = types_nbtree::BTMaxStrategyNumber;

/// Install every seam declared in `backend-access-index-amapi-seams`.
pub fn init_seams() {
    sx::get_index_am_routine::set(get_index_am_routine);
    sx::get_index_am_info::set(get_index_am_info);
    sx::index_am_translate_strategy::set(index_am_translate_strategy);
    sx::index_am_translate_cmptype::set(index_am_translate_cmptype);
    sx::index_am_canbackward::set(index_am_canbackward);
    sx::index_am_consistent_equality::set(index_am_consistent_equality);
    sx::index_am_consistent_ordering::set(index_am_consistent_ordering);
    sx::index_am_clusterable::set(index_am_clusterable);
    sx::index_am_canorder::set(index_am_canorder);
    sx::index_am_searcharray::set(index_am_searcharray);
    sx::index_am_has_gettuple::set(index_am_has_gettuple);
}

// ===========================================================================
// GetIndexAmRoutine / GetIndexAmRoutineByAmId
// ===========================================================================

/// `GetIndexAmRoutine(amhandler)` (amapi.c) — call the specified access-method
/// handler routine to get its `IndexAmRoutine` struct. Built-in handlers
/// involve no catalog access, so the dispatch matches the handler-function OID
/// to the built-in AM handler that the fmgr builtin table would reach.
pub fn GetIndexAmRoutine(amhandler: Oid) -> PgResult<IndexAmRoutine> {
    // datum = OidFunctionCall0(amhandler);
    // routine = (IndexAmRoutine *) DatumGetPointer(datum);
    let routine = match amhandler {
        F_BTHANDLER => backend_access_nbtree_nbtree::bthandler(),
        F_HASHHANDLER => backend_access_hash_entry::hashhandler(),
        // A handler the built-in fmgr table doesn't carry would be a
        // dynamically loaded extension AM, reached through the (unported)
        // dynamic-fmgr dispatch. `mirror PG and panic` until that lands; a
        // built-in catalog never reaches here.
        other => panic!(
            "index access method handler function {other} is not a built-in \
             handler (dynamic AM handler dispatch is not yet ported)"
        ),
    };

    // if (routine == NULL || !IsA(routine, IndexAmRoutine)) elog(ERROR, ...);
    // The handler returns a value-typed `IndexAmRoutine` (never NULL) whose
    // `type_` the handler stamps as `T_IndexAmRoutine`; an extension handler
    // returning the wrong tag would be the C `did not return an IndexAmRoutine`
    // error. The built-in handlers above always stamp it correctly.
    Ok(routine)
}

/// `GetIndexAmRoutineByAmId(amoid, noerror = false)` (amapi.c) — look up the
/// handler of the index access method with the given OID and get its
/// `IndexAmRoutine`. The repo always calls this with `noerror = false`, so the
/// not-found cases raise the C errors.
pub fn GetIndexAmRoutineByAmId(amoid: Oid) -> PgResult<IndexAmRoutine> {
    // tuple = SearchSysCache1(AMOID, ObjectIdGetDatum(amoid));
    // amform = (Form_pg_am) GETSTRUCT(tuple); amhandler = amform->amhandler;
    // The index-AM-specific projection folds the `amtype != AMTYPE_INDEX`
    // rejection into the lookup.
    let amhandler = match syscache::search_am_handler::call(amoid)? {
        Some(h) => h,
        None => {
            // if (!HeapTupleIsValid(tuple)) elog(ERROR, "cache lookup failed
            // for access method %u", amoid);
            return Err(PgError::error(format!(
                "cache lookup failed for access method {amoid}"
            )));
        }
    };

    // if (!RegProcedureIsValid(amhandler)) ereport(ERROR, ...);
    if amhandler == types_core::primitive::InvalidOid {
        return Err(PgError::error(format!(
            "index access method {amoid} does not have a handler"
        ))
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE));
    }

    // return GetIndexAmRoutine(amhandler);
    GetIndexAmRoutine(amhandler)
}

// ===========================================================================
// IndexAmTranslateStrategy / IndexAmTranslateCompareType
// ===========================================================================

/// `IndexAmTranslateStrategy(strategy, amoid, opfamily, missing_ok)` (amapi.c)
/// — given an access method and strategy, get the corresponding compare type.
pub fn IndexAmTranslateStrategy(
    strategy: StrategyNumber,
    amoid: Oid,
    opfamily: Oid,
    missing_ok: bool,
) -> PgResult<CompareType> {
    // shortcut for common case
    if amoid == BTREE_AM_OID
        && strategy > InvalidStrategy
        && strategy <= BTMaxStrategyNumber
    {
        // return (CompareType) strategy;
        return Ok(compare_type_from_i32(strategy as i32));
    }

    let amroutine = GetIndexAmRoutineByAmId(amoid)?;
    let result = match amroutine.amtranslatestrategy {
        Some(f) => f(strategy, opfamily),
        None => COMPARE_INVALID,
    };

    if !missing_ok && result == COMPARE_INVALID {
        return Err(PgError::error(format!(
            "could not translate strategy number {strategy} for index AM {amoid}"
        )));
    }

    Ok(result)
}

/// `IndexAmTranslateCompareType(cmptype, amoid, opfamily, missing_ok)` (amapi.c)
/// — given an access method and compare type, get the corresponding strategy
/// number.
pub fn IndexAmTranslateCompareType(
    cmptype: CompareType,
    amoid: Oid,
    opfamily: Oid,
    missing_ok: bool,
) -> PgResult<StrategyNumber> {
    // shortcut for common case (the C compares the enum values numerically:
    // `cmptype > COMPARE_INVALID && cmptype <= COMPARE_GT`).
    let cmpval = cmptype as i32;
    if amoid == BTREE_AM_OID
        && cmpval > (COMPARE_INVALID as i32)
        && cmpval <= (COMPARE_GT as i32)
    {
        // return (StrategyNumber) cmptype;
        return Ok(cmpval as StrategyNumber);
    }

    let amroutine = GetIndexAmRoutineByAmId(amoid)?;
    let result = match amroutine.amtranslatecmptype {
        Some(f) => f(cmptype, opfamily),
        None => InvalidStrategy,
    };

    if !missing_ok && result == InvalidStrategy {
        return Err(PgError::error(format!(
            "could not translate compare type {} for index AM {amoid}",
            cmptype as i32
        )));
    }

    Ok(result)
}

/// `(CompareType) strategy` — reconstruct the [`CompareType`] enum from its
/// integer value (the btree fast path returns the strategy number directly,
/// which is numerically the compare type).
fn compare_type_from_i32(v: i32) -> CompareType {
    use types_tableam::amapi::CompareType::*;
    match v {
        0 => COMPARE_INVALID,
        1 => COMPARE_LT,
        2 => COMPARE_LE,
        3 => COMPARE_EQ,
        4 => COMPARE_GE,
        5 => COMPARE_GT,
        6 => COMPARE_NE,
        7 => COMPARE_OVERLAP,
        8 => COMPARE_CONTAINED_BY,
        other => panic!("invalid CompareType value {other}"),
    }
}

// ===========================================================================
// Seam implementations: caller-shaped projections of the routine
// ===========================================================================

/// `get_index_am_routine(amhandler)` — the relcache `InitIndexAmRoutine` path:
/// call the handler and return the vtable to cache in `rd_indam`.
fn get_index_am_routine(amhandler: Oid) -> PgResult<IndexAmRoutine> {
    GetIndexAmRoutine(amhandler)
}

/// `get_index_am_info(amoid)` — `GetIndexAmRoutineByAmId(amoid, false)` projected
/// to the scalar `IndexAmRoutine` fields opclasscmds.c reads.
fn get_index_am_info(amoid: Oid) -> PgResult<IndexAmInfo> {
    let amroutine = GetIndexAmRoutineByAmId(amoid)?;
    Ok(IndexAmInfo {
        amstrategies: amroutine.amstrategies as i32,
        amsupport: amroutine.amsupport as i32,
        amoptsprocnum: amroutine.amoptsprocnum as i32,
        amstorage: amroutine.amstorage,
        amcanorder: amroutine.amcanorder,
        amcanhash: amroutine.amcanhash,
        amcanorderbyop: amroutine.amcanorderbyop,
        // `amroutine->amadjustmembers != NULL`. The unified vtable does not
        // carry the `amadjustmembers` callback (it is reached by name from the
        // AM's validate crate, which returns a soft-error result that cannot be
        // a raw fn-ptr), so the projection derives it from the AM: the built-in
        // btree and hash AMs both define `amadjustmembers`.
        has_adjustmembers: am_has_adjustmembers(amoid),
    })
}

/// Whether the AM defines `amadjustmembers`. Built-in btree/hash both do; an
/// AM resolvable through `GetIndexAmRoutineByAmId` that is neither is a future
/// extension AM whose vtable would need to carry the flag.
fn am_has_adjustmembers(amoid: Oid) -> bool {
    matches!(amoid, 403 | 405)
}

/// `index_am_translate_strategy(strategy, amoid, opfamily, missing_ok)` — the
/// `IndexAmTranslateStrategy` result as its `i32` compare-type value.
fn index_am_translate_strategy(
    strategy: i32,
    amoid: Oid,
    opfamily: Oid,
    missing_ok: bool,
) -> PgResult<i32> {
    Ok(IndexAmTranslateStrategy(strategy as StrategyNumber, amoid, opfamily, missing_ok)? as i32)
}

/// `index_am_translate_cmptype(cmptype, amoid, opfamily, missing_ok)` — the
/// `IndexAmTranslateCompareType` result as an `i16` strategy number.
fn index_am_translate_cmptype(
    cmptype: i32,
    amoid: Oid,
    opfamily: Oid,
    missing_ok: bool,
) -> PgResult<i16> {
    Ok(IndexAmTranslateCompareType(compare_type_from_i32(cmptype), amoid, opfamily, missing_ok)?
        as i16)
}

/// `GetIndexAmRoutineByAmId(amoid, false)->amcanbackward`.
fn index_am_canbackward(amoid: Oid) -> PgResult<bool> {
    Ok(GetIndexAmRoutineByAmId(amoid)?.amcanbackward)
}

/// `GetIndexAmRoutineByAmId(amoid, false)->amconsistentequality`.
fn index_am_consistent_equality(amoid: Oid) -> PgResult<bool> {
    Ok(GetIndexAmRoutineByAmId(amoid)?.amconsistentequality)
}

/// `GetIndexAmRoutineByAmId(amoid, false)->amconsistentordering`.
fn index_am_consistent_ordering(amoid: Oid) -> PgResult<bool> {
    Ok(GetIndexAmRoutineByAmId(amoid)?.amconsistentordering)
}

/// `GetIndexAmRoutineByAmId(amoid, false)->amclusterable`.
fn index_am_clusterable(amoid: Oid) -> PgResult<bool> {
    Ok(GetIndexAmRoutineByAmId(amoid)?.amclusterable)
}

/// `GetIndexAmRoutineByAmId(amoid, false)->amcanorder`.
fn index_am_canorder(amoid: Oid) -> PgResult<bool> {
    Ok(GetIndexAmRoutineByAmId(amoid)?.amcanorder)
}

/// `GetIndexAmRoutineByAmId(amoid, false)->amsearcharray`.
fn index_am_searcharray(amoid: Oid) -> PgResult<bool> {
    Ok(GetIndexAmRoutineByAmId(amoid)?.amsearcharray)
}

/// `GetIndexAmRoutineByAmId(amoid, false)->amgettuple != NULL`.
fn index_am_has_gettuple(amoid: Oid) -> PgResult<bool> {
    Ok(GetIndexAmRoutineByAmId(amoid)?.amgettuple.is_some())
}
