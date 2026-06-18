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
use backend_utils_adt_amutils_seams as amutils_sx;
use backend_utils_cache_syscache_seams as syscache;

use mcx::Mcx;

/// `BTREE_AM_OID` (catalog/pg_am.dat) — the built-in btree access method.
const BTREE_AM_OID: Oid = 403;

/// `HASH_AM_OID` (catalog/pg_am.dat) — the built-in hash access method.
const HASH_AM_OID: Oid = 405;

/// `GIST_AM_OID` (catalog/pg_am.dat) — the built-in GiST access method.
const GIST_AM_OID: Oid = 783;

/// `GIN_AM_OID` (catalog/pg_am.dat) — the built-in GIN access method.
const GIN_AM_OID: Oid = 2742;

/// `BRIN_AM_OID` (catalog/pg_am.dat) — the built-in BRIN access method.
const BRIN_AM_OID: Oid = 3580;

/// `SPGIST_AM_OID` (catalog/pg_am.dat) — the built-in SP-GiST access method.
const SPGIST_AM_OID: Oid = 4000;

/// `F_BTHANDLER` (pg_proc.dat oid 330) — the btree AM handler function.
const F_BTHANDLER: Oid = 330;
/// `F_HASHHANDLER` (pg_proc.dat oid 331) — the hash AM handler function.
const F_HASHHANDLER: Oid = 331;
/// `F_GISTHANDLER` (pg_proc.dat oid 332) — the GiST AM handler function.
const F_GISTHANDLER: Oid = 332;
/// `F_GINHANDLER` (pg_proc.dat oid 333) — the GIN AM handler function.
const F_GINHANDLER: Oid = 333;
/// `F_SPGHANDLER` (pg_proc.dat oid 334) — the SP-GiST AM handler function.
const F_SPGHANDLER: Oid = 334;
/// `F_BRINHANDLER` (pg_proc.dat oid 335) — the BRIN AM handler function.
const F_BRINHANDLER: Oid = 335;

/// `BTMaxStrategyNumber` (access/nbtree.h) — the btree fast-path bound used by
/// `IndexAmTranslateStrategy`.
const BTMaxStrategyNumber: StrategyNumber = types_nbtree::BTMaxStrategyNumber;

/// Install every seam declared in `backend-access-index-amapi-seams`.
pub fn init_seams() {
    sx::get_index_am_routine::set(get_index_am_routine);
    sx::get_index_am_routine_by_amid::set(get_index_am_routine_by_amid);
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
    sx::am_adjust_members::set(am_adjust_members);

    // amutils.c SQL-level property reporting: the AM-routine projection, the
    // per-AM `amproperty` / `ambuildphasename` callbacks (which the unified
    // `IndexAmRoutine` vtable does not carry — dispatched by AM OID by name,
    // as `amvalidate` is), and the generic `index_can_return` fallback.
    amutils_sx::am_routine::set(amutils_am_routine);
    amutils_sx::am_property::set(amutils_am_property);
    amutils_sx::index_can_return::set(amutils_index_can_return);
    amutils_sx::am_buildphasename::set(amutils_am_buildphasename);

    // Register this crate's SQL-callable fmgr builtins (C: `fmgr_builtins[]`).
    fmgr_builtins::register_amapi_builtins();
}

mod fmgr_builtins;

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
        F_GISTHANDLER => backend_access_gist_core::gisthandler(),
        F_GINHANDLER => backend_access_gin_ginutil::ginhandler(),
        F_SPGHANDLER => backend_access_spgist_core::spghandler(),
        F_BRINHANDLER => backend_access_brin_scan::brinhandler(),
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

// ===========================================================================
// amvalidate (SQL-callable: validate an opclass via its AM)
// ===========================================================================

/// `amvalidate(PG_FUNCTION_ARGS)` (amapi.c, pg_proc oid 2871) — the SQL-callable
/// function that asks the appropriate access method to validate the specified
/// opclass. It looks up the opclass in the `CLAOID` syscache, reads `opcmethod`
/// (the AM oid), loads the AM's `IndexAmRoutine` via
/// `GetIndexAmRoutineByAmId(amoid, false)`, then dispatches
/// `amroutine->amvalidate(opclassoid)` (elog ERROR if the AM has no
/// `amvalidate`) and `PG_RETURN_BOOL(result)`.
///
/// The C `amroutine->amvalidate` callback cannot be carried as a raw fn-ptr in
/// the unified `IndexAmRoutine` vtable (it returns a soft-error `PgResult` and
/// needs an `Mcx`, the same reason `amadjustmembers` is reached by name rather
/// than through the vtable — see `am_has_adjustmembers`; nbtree/hash both stamp
/// `amvalidate: None`). So the dispatch resolves the AM oid to the built-in
/// AM's `*validate` routine directly, which is exactly the function the AM's
/// `bthandler` / `hashhandler` / `ginhandler` / `brinhandler` stores in
/// `amvalidate`. An AM oid whose validator is not yet ported (SP-GiST / GiST)
/// or a future extension AM whose validator is reached through the (unported)
/// dynamic-fmgr dispatch maps to the C `function amvalidate is not defined`
/// error.
pub fn amvalidate(mcx: mcx::Mcx<'_>, opclassoid: Oid) -> PgResult<bool> {
    // classtup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassoid));
    // if (!HeapTupleIsValid(classtup)) elog(ERROR, "cache lookup failed for
    //     operator class %u", opclassoid);
    // classform = (Form_pg_opclass) GETSTRUCT(classtup);
    // amoid = classform->opcmethod;
    // ReleaseSysCache(classtup);
    let amoid = match syscache::pg_opclass_form::call(opclassoid)? {
        // pg_opclass_form projects (opcfamily, opcintype, opcmethod).
        Some((_opcfamily, _opcintype, opcmethod)) => opcmethod,
        None => {
            return Err(PgError::error(format!(
                "cache lookup failed for operator class {opclassoid}"
            )));
        }
    };

    // amroutine = GetIndexAmRoutineByAmId(amoid, false);
    // The load validates the AM oid (raising the C `cache lookup failed` /
    // `does not have a handler` errors).
    let _amroutine = GetIndexAmRoutineByAmId(amoid)?;

    // if (amroutine->amvalidate == NULL) elog(ERROR, "function amvalidate is
    //     not defined for index access method %u", amoid);
    // result = amroutine->amvalidate(opclassoid);
    // pfree(amroutine);
    let result = match amoid {
        BTREE_AM_OID => backend_access_nbt_validate::btvalidate(mcx, opclassoid)?,
        HASH_AM_OID => backend_access_hashvalidate::hashvalidate(mcx, opclassoid)?,
        GIN_AM_OID => {
            backend_access_gin_core_probe::ginvalidate::ginvalidate(mcx, opclassoid)?
        }
        BRIN_AM_OID => backend_access_brin_validate::brinvalidate(mcx, opclassoid)?,
        GIST_AM_OID => backend_access_gist_validate::gistvalidate(mcx, opclassoid)?,
        SPGIST_AM_OID => backend_access_spg_validate::spgvalidate(mcx, opclassoid)?,
        // A future extension AM whose validator is reached through the
        // (unported) dynamic-fmgr dispatch maps to the C `function amvalidate
        // is not defined for index access method %u` error.
        _ => {
            return Err(PgError::error(format!(
                "function amvalidate is not defined for index access method \
                 {amoid}"
            )));
        }
    };

    // PG_RETURN_BOOL(result);
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

/// `get_index_am_routine_by_amid(amoid)` — `GetIndexAmRoutineByAmId(amoid,
/// false)`: resolve the AM's `amhandler` from pg_am, call it, and return the
/// full `IndexAmRoutine` vtable (read by `catalog/index.c`
/// `ConstructTupleDescriptor` for `amroutine->amkeytype`).
fn get_index_am_routine_by_amid(amoid: Oid) -> PgResult<IndexAmRoutine> {
    GetIndexAmRoutineByAmId(amoid)
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

/// Whether the AM defines `amadjustmembers`. The built-in btree (403), hash
/// (405), GiST (783), GIN (2742) and SP-GiST (4000) AMs all set
/// `amroutine->amadjustmembers` (nbtree.c / hash.c / gist.c / ginutil.c /
/// spgutils.c); BRIN (3580) sets it to NULL (brin.c). An AM resolvable through
/// `GetIndexAmRoutineByAmId` that is none of these is a future extension AM whose
/// vtable would need to carry the flag.
fn am_has_adjustmembers(amoid: Oid) -> bool {
    matches!(
        amoid,
        BTREE_AM_OID | HASH_AM_OID | GIST_AM_OID | GIN_AM_OID | SPGIST_AM_OID
    )
}

/// `am_adjust_members` seam — the `amroutine->amadjustmembers(opfamilyoid,
/// opclassoid, operators, procedures)` dispatch (driven by opclasscmds.c's
/// `AddOpClass` / `AlterOpFamilyAdd`). Each AM's member-adjustment callback sets
/// the dependency-strength fields (`ref_is_hard` / `ref_is_family` / `refobjid`)
/// on the operators and support functions being added to an opclass/opfamily and
/// may additionally validate them.
///
/// The unified `IndexAmRoutine` vtable's `amadjustmembers` slot is not used here:
/// the real callbacks live in the per-AM opclass-validator crates and several of
/// them (hash, gin) carry a trimmed per-AM `OpFamilyMember` shape rather than the
/// seam's canonical `types_opclass::OpFamilyMember`. We dispatch by `amoid` to
/// the validator body directly. For the trimmed-shape AMs we marshal the shared
/// fields the callback reads (`is_func`/`number`/`lefttype`/`righttype`) into the
/// per-AM record, run it, and merge the three dependency fields it mutates back
/// into the canonical members — leaving `object`/`sortfamily` (which the callback
/// neither reads nor writes) intact for the consumer's `storeOperators`.
fn am_adjust_members<'mcx>(
    mcx: Mcx<'mcx>,
    amoid: Oid,
    opfamilyoid: Oid,
    opclassoid: Oid,
    mut operators: mcx::PgVec<'mcx, types_opclass::OpFamilyMember>,
    mut procedures: mcx::PgVec<'mcx, types_opclass::OpFamilyMember>,
) -> PgResult<(
    mcx::PgVec<'mcx, types_opclass::OpFamilyMember>,
    mcx::PgVec<'mcx, types_opclass::OpFamilyMember>,
)> {
    match amoid {
        // btree / GiST / SP-GiST callbacks already operate on the canonical
        // `types_opclass::OpFamilyMember` — call them in place.
        BTREE_AM_OID => {
            backend_access_nbt_validate::btadjustmembers(
                opfamilyoid,
                opclassoid,
                &mut operators,
                &mut procedures,
            )?;
        }
        GIST_AM_OID => {
            backend_access_gist_validate::gistadjustmembers(
                opfamilyoid,
                opclassoid,
                &mut operators,
                &mut procedures,
            )?;
        }
        SPGIST_AM_OID => {
            backend_access_spg_validate::spgadjustmembers(
                opfamilyoid,
                opclassoid,
                &mut operators,
                &mut procedures,
            )?;
        }
        // hash / GIN callbacks carry the trimmed per-AM `OpFamilyMember`; marshal
        // in, run, and merge the mutated dependency fields back.
        HASH_AM_OID => {
            let mut ops = to_trimmed(&operators);
            let mut procs = to_trimmed(&procedures);
            backend_access_hashvalidate::hashadjustmembers(
                opfamilyoid,
                opclassoid,
                &mut ops,
                &mut procs,
            )?;
            merge_trimmed_deps(&mut operators, &ops);
            merge_trimmed_deps(&mut procedures, &procs);
        }
        GIN_AM_OID => {
            let mut ops = to_trimmed(&operators);
            let mut procs = to_trimmed(&procedures);
            backend_access_gin_core_probe::ginvalidate::ginadjustmembers(
                opfamilyoid,
                opclassoid,
                &mut ops,
                &mut procs,
            )?;
            merge_trimmed_deps(&mut operators, &ops);
            merge_trimmed_deps(&mut procedures, &procs);
        }
        // BRIN has no amadjustmembers (the consumer guards on
        // `has_adjustmembers`, so this is unreachable for it); any other AM is a
        // future extension reached through the unported dynamic dispatch.
        other => panic!(
            "index access method {other} does not provide a built-in \
             amadjustmembers callback (dynamic AM dispatch is not yet ported)"
        ),
    }

    let _ = mcx;
    Ok((operators, procedures))
}

/// `types_hash::...::OpFamilyMember` — the trimmed per-AM record the hash/GIN
/// `amadjustmembers` callbacks mutate.
type TrimmedMember = types_hash::backend_access_hash_hashvalidate::OpFamilyMember;

/// Marshal the canonical seam members into the trimmed per-AM record, copying
/// the fields the hash/GIN callbacks read (`is_func`/`number`/`lefttype`/
/// `righttype`) plus the current dependency fields.
fn to_trimmed(members: &[types_opclass::OpFamilyMember]) -> std::vec::Vec<TrimmedMember> {
    members
        .iter()
        .map(|m| TrimmedMember {
            is_func: m.is_func,
            number: m.number as i16,
            lefttype: m.lefttype,
            righttype: m.righttype,
            ref_is_hard: m.ref_is_hard,
            ref_is_family: m.ref_is_family,
            refobjid: m.refobjid,
        })
        .collect()
}

/// Merge the dependency fields the callback mutated (`ref_is_hard` /
/// `ref_is_family` / `refobjid`) back into the canonical members; the callbacks
/// touch nothing else, so `object`/`sortfamily` are preserved.
fn merge_trimmed_deps(canonical: &mut [types_opclass::OpFamilyMember], trimmed: &[TrimmedMember]) {
    debug_assert_eq!(canonical.len(), trimmed.len());
    for (dst, src) in canonical.iter_mut().zip(trimmed.iter()) {
        dst.ref_is_hard = src.ref_is_hard;
        dst.ref_is_family = src.ref_is_family;
        dst.refobjid = src.refobjid;
    }
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

// ===========================================================================
// Seam implementations: amutils.c (SQL-level index AM property reporting)
// ===========================================================================

/// `GetIndexAmRoutineByAmId(amoid, noerror = true)` — the `noerror` variant
/// amutils.c uses. Returns `Ok(None)` for the C `routine == NULL` cases (the
/// AM's `pg_am` row or handler is missing); `GetIndexAmRoutineByAmId(amoid,
/// false)` would instead raise the `cache lookup failed` / `does not have a
/// handler` errors.
fn get_index_am_routine_by_am_id_noerror(amoid: Oid) -> PgResult<Option<IndexAmRoutine>> {
    // tuple = SearchSysCache1(AMOID, ObjectIdGetDatum(amoid));
    // if (!HeapTupleIsValid(tuple)) { if (noerror) return NULL; ... }
    let amhandler = match syscache::search_am_handler::call(amoid)? {
        Some(h) => h,
        None => return Ok(None),
    };

    // if (!RegProcedureIsValid(amhandler)) { if (noerror) return NULL; ... }
    if amhandler == types_core::primitive::InvalidOid {
        return Ok(None);
    }

    // return GetIndexAmRoutine(amhandler);
    Ok(Some(GetIndexAmRoutine(amhandler)?))
}

/// amutils.c: `GetIndexAmRoutineByAmId(amoid, true)` projected to the scalar
/// capability flags + `routine->amX != NULL` "callback present" booleans the
/// `indexam_property` decision tree reads. `Ok(None)` for the missing-AM
/// path. The `has_amproperty` / `has_ambuildphasename` booleans are derived
/// from the AM (the unified vtable does not carry those callbacks — they are
/// dispatched by AM OID by name, mirroring `amvalidate` / `amadjustmembers`):
/// btree defines both `btproperty` and `btbuildphasename`; gist/spgist define
/// `gistproperty` / `spgproperty` (no `*buildphasename`); gin defines
/// `ginbuildphasename` (no `*property`); brin/hash define neither — exactly the
/// C `bthandler` / `gisthandler` / `spghandler` / `ginhandler` / `brinhandler`
/// / `hashhandler` assignments.
fn amutils_am_routine(amoid: Oid) -> PgResult<Option<amutils_sx::IndexAmRoutineFlags>> {
    let routine = match get_index_am_routine_by_am_id_noerror(amoid)? {
        Some(r) => r,
        None => return Ok(None),
    };

    // routine->amproperty != NULL / routine->ambuildphasename != NULL: derived
    // from the AM, since the unified vtable does not carry those callbacks.
    let (has_amproperty, has_ambuildphasename) = match amoid {
        BTREE_AM_OID => (true, true),
        GIST_AM_OID => (true, false),
        SPGIST_AM_OID => (true, false),
        GIN_AM_OID => (false, true),
        BRIN_AM_OID => (false, false),
        HASH_AM_OID => (false, false),
        // A future extension AM would carry the flags in its own vtable.
        _ => (false, false),
    };

    Ok(Some(amutils_sx::IndexAmRoutineFlags {
        amcanorder: routine.amcanorder,
        amcanorderbyop: routine.amcanorderbyop,
        amcanbackward: routine.amcanbackward,
        amcanunique: routine.amcanunique,
        amcanmulticol: routine.amcanmulticol,
        amsearcharray: routine.amsearcharray,
        amsearchnulls: routine.amsearchnulls,
        amclusterable: routine.amclusterable,
        amcaninclude: routine.amcaninclude,
        has_amproperty,
        has_amcanreturn: routine.amcanreturn.is_some(),
        has_amgettuple: routine.amgettuple.is_some(),
        has_amgetbitmap: routine.amgetbitmap.is_some(),
        has_ambuildphasename,
    }))
}

/// amutils.c: `routine->amproperty(index_oid, attno, prop, propname, &res,
/// &isnull)` — the AM's optional property callback, dispatched by AM OID by
/// name. Returns `Ok(None)` for the C `false` (not handled — fall through to
/// the generic logic) and `Ok(Some((res, isnull)))` for the C `true`.
///
/// Only the AMs that assign a non-NULL `amproperty` in C reach a real callback:
/// btree (`btproperty`), gist (`gistproperty`), spgist (`spgproperty`). The
/// caller (`indexam_property`) only invokes this seam when `has_amproperty` is
/// true, so the other AMs are unreachable here; they map to "not handled".
fn amutils_am_property(
    mcx: Mcx<'_>,
    req: amutils_sx::AmPropertyRequest,
) -> PgResult<Option<(bool, bool)>> {
    match req.amoid {
        BTREE_AM_OID => {
            // btproperty handles only AMPROP_RETURNABLE; everything else punts.
            use backend_access_nbtree_core::utils::IndexAMProperty as BtProp;
            let bt_prop = match req.prop {
                amutils_sx::IndexAmProperty::Returnable => BtProp::AmpropReturnable,
                _ => BtProp::Other,
            };
            let mut res = false;
            let mut isnull = false;
            let handled = backend_access_nbtree_core::utils::btproperty(
                req.index_oid,
                req.attno,
                bt_prop,
                &req.propname,
                &mut res,
                &mut isnull,
            );
            Ok(if handled { Some((res, isnull)) } else { None })
        }
        GIST_AM_OID => {
            use backend_access_gist_core::gistutil::IndexAMProperty as GiProp;
            let gi_prop = match req.prop {
                amutils_sx::IndexAmProperty::DistanceOrderable => GiProp::DistanceOrderable,
                amutils_sx::IndexAmProperty::Returnable => GiProp::Returnable,
                _ => GiProp::Other,
            };
            let (handled, res, isnull) =
                backend_access_gist_core::gistutil::gistproperty(req.index_oid, req.attno, gi_prop)?;
            Ok(if handled { Some((res, isnull)) } else { None })
        }
        SPGIST_AM_OID => {
            use backend_access_spgist_core::IndexAMProperty as SpProp;
            let sp_prop = match req.prop {
                amutils_sx::IndexAmProperty::DistanceOrderable => SpProp::DistanceOrderable,
                _ => SpProp::Other,
            };
            let (handled, res, isnull) =
                backend_access_spgist_core::spgproperty(mcx, req.index_oid, req.attno, sp_prop)?;
            Ok(if handled { Some((res, isnull)) } else { None })
        }
        // An AM that assigns amproperty = NULL in C never reaches here (the
        // caller gates on has_amproperty), and a future extension AM's callback
        // would be reached through the (unported) dynamic-fmgr dispatch.
        _ => Ok(None),
    }
}

/// amutils.c: the generic `AMPROP_RETURNABLE` fallback —
/// `indexrel = index_open(index_oid, AccessShareLock);`
/// `res = index_can_return(indexrel, attno);`
/// `index_close(indexrel, AccessShareLock);`
fn amutils_index_can_return(mcx: Mcx<'_>, index_oid: Oid, attno: i32) -> PgResult<bool> {
    use backend_access_index_indexam as indexam;
    use types_storage::lock::AccessShareLock;

    let indexrel = indexam::index_open(mcx, index_oid, AccessShareLock)?;
    let res = indexam::index_can_return(&indexrel, attno)?;
    indexam::index_close(indexrel, AccessShareLock)?;
    Ok(res)
}

/// amutils.c: `name = routine->ambuildphasename(phasenum);` then
/// `CStringGetTextDatum(name)` (or NULL). Dispatched by AM OID by name (the
/// unified vtable does not carry `ambuildphasename`). The caller only invokes
/// this when `has_ambuildphasename` is true. Only btree (`btbuildphasename`)
/// and gin (`ginbuildphasename`) assign a non-NULL `ambuildphasename` in C.
fn amutils_am_buildphasename(amoid: Oid, phasenum: i64) -> PgResult<Option<String>> {
    let name: Option<&'static str> = match amoid {
        BTREE_AM_OID => backend_access_nbtree_core::utils::btbuildphasename(phasenum),
        GIN_AM_OID => backend_access_gin_ginutil::ginbuildphasename(phasenum),
        // No other built-in AM assigns ambuildphasename; the caller gates on
        // has_ambuildphasename so this is unreachable for them.
        _ => None,
    };
    Ok(name.map(String::from))
}
