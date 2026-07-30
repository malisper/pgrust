#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use cache_syscache::cacheinfo::AMOID;
use cache_syscache::{ReleaseSysCache, SearchSysCache1, SysCacheGetAttrNotNull, SysCacheKey};
use datum::Datum;
use types_core::{InvalidOid, Oid, BTREE_AM_OID};
use types_error::{PgError, PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE};
use types_pathnodes::{CompareType, COMPARE_GT, COMPARE_INVALID};
use types_relscan::IndexAmKind;
use types_scan::scankey::{BTMaxStrategyNumber, InvalidStrategy, StrategyNumber};

pub const F_BTHANDLER: Oid = 330;
pub const F_HASHHANDLER: Oid = 331;
pub const F_GINHANDLER: Oid = 333;
pub const F_GISTHANDLER: Oid = 332;
pub const F_SPGHANDLER: Oid = 334;
pub const F_BRINHANDLER: Oid = 335;
const AMTYPE_INDEX: i8 = b'i' as i8;
const Anum_pg_am_amname: i32 = 2;
const Anum_pg_am_amhandler: i32 = 3;
const Anum_pg_am_amtype: i32 = 4;
const NAMEDATALEN: usize = 64;

/// C calls the handler by OID and gets a palloc'd IndexAmRoutine; the closed
/// AM set makes the routine the IndexAmKind enum. No catalog access, so safe
/// while bootstrapping catalog indexes (relcache relies on that).
pub fn GetIndexAmRoutine(amhandler: Oid) -> IndexAmKind {
    match amhandler {
        F_BTHANDLER => IndexAmKind::Btree,
        F_HASHHANDLER => IndexAmKind::Hash,
        F_GINHANDLER => IndexAmKind::Gin,
        F_GISTHANDLER => IndexAmKind::Gist,
        F_SPGHANDLER => IndexAmKind::Spgist,
        F_BRINHANDLER => IndexAmKind::Brin,
        other => resolve_extension_handler(other),
    }
}

// Non-builtin amhandler (extension AM): map by the handler proc's C symbol.
// Catalog access is fine here — bootstrap only reaches the builtin arms.
fn resolve_extension_handler(amhandler: Oid) -> IndexAmKind {
    if let Ok(Some(name)) = syscache_seams::pg_proc_proname::call(amhandler) {
        if name.name_str() == b"hnswhandler" {
            return IndexAmKind::Hnsw;
        }
        if name.name_str() == b"blhandler" {
            return IndexAmKind::Bloom;
        }
    }
    unported_handler(amhandler)
}

pub fn GetIndexAmRoutineByAmId(amoid: Oid, noerror: bool) -> PgResult<Option<IndexAmKind>> {
    let Some(tuple) = SearchSysCache1(AMOID, SysCacheKey::Value(Datum::from_oid(amoid)))? else {
        if noerror {
            return Ok(None);
        }
        return Err(am_lookup_failed(amoid));
    };

    let amtype = SysCacheGetAttrNotNull(AMOID, &tuple, Anum_pg_am_amtype)?.as_char();
    if amtype != AMTYPE_INDEX {
        if noerror {
            ReleaseSysCache(tuple);
            return Ok(None);
        }
        let name = am_name(&tuple)?;
        ReleaseSysCache(tuple);
        return Err(not_index_am(name));
    }

    let amhandler = SysCacheGetAttrNotNull(AMOID, &tuple, Anum_pg_am_amhandler)?.as_oid();
    if amhandler == InvalidOid {
        if noerror {
            ReleaseSysCache(tuple);
            return Ok(None);
        }
        let name = am_name(&tuple)?;
        ReleaseSysCache(tuple);
        return Err(no_handler(name));
    }

    ReleaseSysCache(tuple);
    Ok(Some(GetIndexAmRoutine(amhandler)))
}

pub fn IndexAmTranslateStrategy(
    strategy: StrategyNumber,
    amoid: Oid,
    opfamily: Oid,
    missing_ok: bool,
) -> PgResult<CompareType> {
    let _ = opfamily;
    if amoid == BTREE_AM_OID && strategy > InvalidStrategy && strategy <= BTMaxStrategyNumber {
        return Ok(strategy as CompareType);
    }

    let kind = GetIndexAmRoutineByAmId(amoid, false)?.expect("noerror=false returned Some");
    let result = match kind {
        // bttranslatestrategy outside 1..=5 (the shortcut covered the rest).
        IndexAmKind::Btree => COMPARE_INVALID,
        // hashtranslatestrategy: only HTEqualStrategyNumber -> COMPARE_EQ.
        IndexAmKind::Hash => {
            if strategy == 1 {
                types_pathnodes::COMPARE_EQ
            } else {
                COMPARE_INVALID
            }
        }
        // gin has no amtranslatestrategy.
        IndexAmKind::Gin => COMPARE_INVALID,
        // amtranslatestrategy == NULL for gist.
        IndexAmKind::Gist => COMPARE_INVALID,
        // amtranslatestrategy == NULL.
        IndexAmKind::Spgist => COMPARE_INVALID,
        // amtranslatestrategy == NULL.
        IndexAmKind::Brin => COMPARE_INVALID,
        IndexAmKind::Hnsw => COMPARE_INVALID,
        // amtranslatestrategy == NULL (contrib/bloom blhandler).
        IndexAmKind::Bloom => COMPARE_INVALID,
        #[allow(unreachable_patterns)]
        _ => unported_translate(amoid),
    };

    if !missing_ok && result == COMPARE_INVALID {
        return Err(Box::new(PgError::error(format!(
            "could not translate strategy number {strategy} for index AM {amoid}"
        ))));
    }
    Ok(result)
}

pub fn IndexAmTranslateCompareType(
    cmptype: CompareType,
    amoid: Oid,
    opfamily: Oid,
    missing_ok: bool,
) -> PgResult<StrategyNumber> {
    if amoid == BTREE_AM_OID && cmptype > COMPARE_INVALID && cmptype <= COMPARE_GT {
        return Ok(cmptype as StrategyNumber);
    }

    let kind = GetIndexAmRoutineByAmId(amoid, false)?.expect("noerror=false returned Some");
    let result = match kind {
        // bttranslatecmptype outside COMPARE_LT..=COMPARE_GT.
        IndexAmKind::Btree => InvalidStrategy,
        // hashtranslatecmptype: only COMPARE_EQ -> HTEqualStrategyNumber.
        IndexAmKind::Hash => {
            if cmptype == types_pathnodes::COMPARE_EQ {
                1 as StrategyNumber
            } else {
                InvalidStrategy
            }
        }
        // gin has no amtranslatecmptype.
        IndexAmKind::Gin => InvalidStrategy,
        IndexAmKind::Gist => gisttranslatecmptype(cmptype, opfamily)?,
        // amtranslatecmptype == NULL.
        IndexAmKind::Spgist => InvalidStrategy,
        // amtranslatecmptype == NULL.
        IndexAmKind::Brin => InvalidStrategy,
        IndexAmKind::Hnsw => InvalidStrategy,
        // amtranslatecmptype == NULL (contrib/bloom blhandler).
        IndexAmKind::Bloom => InvalidStrategy,
        #[allow(unreachable_patterns)]
        _ => unported_translate(amoid),
    };

    if !missing_ok && result == InvalidStrategy {
        return Err(Box::new(PgError::error(format!(
            "could not translate compare type {cmptype} for index AM {amoid}"
        ))));
    }
    Ok(result)
}

// gist.h
const GIST_TRANSLATE_CMPTYPE_PROC: i16 = 12;

// gisttranslatecmptype (gistutil.c): the opclass's GIST_TRANSLATE_CMPTYPE_PROC
// support function maps CompareType to the opclass's private stratnum;
// InvalidStrategy when the opfamily defines no such proc.
fn gisttranslatecmptype(cmptype: CompareType, opfamily: Oid) -> PgResult<StrategyNumber> {
    let funcid = syscache_seams::lookup_pg_amproc::call(
        opfamily,
        types_core::catalog::ANYOID,
        types_core::catalog::ANYOID,
        GIST_TRANSLATE_CMPTYPE_PROC,
    )?;
    if funcid == InvalidOid {
        return Ok(InvalidStrategy);
    }
    let result =
        fmgr_core::oid_function_call1_coll(funcid, InvalidOid, Datum::from_i32(cmptype))?;
    Ok(result.as_u16())
}

pub fn amvalidate(opclassoid: Oid) -> PgResult<bool> {
    let shape = syscache_seams::lookup_pg_opclass_shape::call(opclassoid)?
        .unwrap_or_else(|| panic!("cache lookup failed for operator class {opclassoid}"));
    let kind = GetIndexAmRoutineByAmId(shape.opcmethod, false)?.expect("noerror=false");
    match kind {
        IndexAmKind::Btree => nbt_validate::btvalidate(opclassoid),
        IndexAmKind::Hash => hash_validate::hashvalidate(opclassoid),
        IndexAmKind::Spgist => spgist_validate::spgvalidate(opclassoid),
        IndexAmKind::Brin => brin_validate::brinvalidate(opclassoid),
        IndexAmKind::Gist => gist_validate::gistvalidate(opclassoid),
        IndexAmKind::Gin => gin_validate::ginvalidate(opclassoid),
        IndexAmKind::Hnsw => pgvector_hnsw::hnswvalidate(opclassoid),
        IndexAmKind::Bloom => bloom::blvalidate(opclassoid),
        #[allow(unreachable_patterns)]
        other => panic!("unported: amvalidate for index AM {other:?}"),
    }
}

// amadjustmembers dispatch (DefineOpClass/AlterOpFamilyAdd).
pub fn am_adjust_members(
    kind: IndexAmKind,
    opfamilyoid: Oid,
    opclassoid: Oid,
    operators: &mut [types_relscan::OpFamilyMember],
    functions: &mut [types_relscan::OpFamilyMember],
) -> PgResult<()> {
    match kind {
        IndexAmKind::Btree => {
            nbt_validate::btadjustmembers(opfamilyoid, opclassoid, operators, functions)
        }
        IndexAmKind::Hash => {
            hash_validate::hashadjustmembers(opfamilyoid, opclassoid, operators, functions)
        }
        IndexAmKind::Spgist => {
            spgist_validate::spgadjustmembers(opfamilyoid, opclassoid, operators, functions)
        }
        IndexAmKind::Gist => gistadjustmembers(opfamilyoid, opclassoid, operators, functions),
        IndexAmKind::Gin => ginadjustmembers(opfamilyoid, opclassoid, operators, functions),
        // C brinhandler sets amadjustmembers = NULL.
        IndexAmKind::Brin => Ok(()),
        IndexAmKind::Hnsw => Ok(()),
        // C blhandler sets amadjustmembers = NULL.
        IndexAmKind::Bloom => Ok(()),
        #[allow(unreachable_patterns)]
        other => panic!("unported: amadjustmembers for index AM {other:?}"),
    }
}

// ginadjustmembers (ginvalidate.c:269): same shape as gistadjustmembers with
// GIN's required (extractValue/extractQuery) vs optional support set.
fn ginadjustmembers(
    opfamilyoid: Oid,
    _opclassoid: Oid,
    operators: &mut [types_relscan::OpFamilyMember],
    functions: &mut [types_relscan::OpFamilyMember],
) -> PgResult<()> {
    for op in operators.iter_mut() {
        op.ref_is_hard = false;
        op.ref_is_family = true;
        op.refobjid = opfamilyoid;
    }
    for op in functions.iter_mut() {
        match op.number as u16 {
            gin_vocab::GIN_EXTRACTVALUE_PROC | gin_vocab::GIN_EXTRACTQUERY_PROC => {
                op.ref_is_hard = true;
            }
            gin_vocab::GIN_COMPARE_PROC
            | gin_vocab::GIN_CONSISTENT_PROC
            | gin_vocab::GIN_COMPARE_PARTIAL_PROC
            | gin_vocab::GIN_TRICONSISTENT_PROC
            | gin_vocab::GIN_OPTIONS_PROC => {
                op.ref_is_hard = false;
                op.ref_is_family = true;
                op.refobjid = opfamilyoid;
            }
            _ => {
                return Err(Box::new(
                    PgError::error(format!(
                        "support function number {} is invalid for access method {}",
                        op.number, "gin"
                    ))
                    .with_sqlstate(types_error::ERRCODE_INVALID_OBJECT_DEFINITION),
                ));
            }
        }
    }
    Ok(())
}

// gistadjustmembers (gistvalidate.c:288): operators always get soft opfamily
// dependencies; required support functions get hard dependencies, optional
// ones soft opfamily dependencies.
fn gistadjustmembers(
    opfamilyoid: Oid,
    _opclassoid: Oid,
    operators: &mut [types_relscan::OpFamilyMember],
    functions: &mut [types_relscan::OpFamilyMember],
) -> PgResult<()> {
    for op in operators.iter_mut() {
        op.ref_is_hard = false;
        op.ref_is_family = true;
        op.refobjid = opfamilyoid;
    }
    for op in functions.iter_mut() {
        match op.number as u16 {
            types_gist::GIST_CONSISTENT_PROC
            | types_gist::GIST_UNION_PROC
            | types_gist::GIST_PENALTY_PROC
            | types_gist::GIST_PICKSPLIT_PROC
            | types_gist::GIST_EQUAL_PROC => {
                op.ref_is_hard = true;
            }
            types_gist::GIST_COMPRESS_PROC
            | types_gist::GIST_DECOMPRESS_PROC
            | types_gist::GIST_DISTANCE_PROC
            | types_gist::GIST_FETCH_PROC
            | types_gist::GIST_OPTIONS_PROC
            | types_gist::GIST_SORTSUPPORT_PROC
            | types_gist::GIST_TRANSLATE_CMPTYPE_PROC => {
                op.ref_is_hard = false;
                op.ref_is_family = true;
                op.refobjid = opfamilyoid;
            }
            _ => {
                return Err(Box::new(
                    PgError::error(format!(
                        "support function number {} is invalid for access method {}",
                        op.number, "gist"
                    ))
                    .with_sqlstate(types_error::ERRCODE_INVALID_OBJECT_DEFINITION),
                ));
            }
        }
    }
    Ok(())
}

fn am_name(tuple: &catcache::CatCTuple) -> PgResult<String> {
    let d = SysCacheGetAttrNotNull(AMOID, tuple, Anum_pg_am_amname)?;
    // SAFETY: amname is a NameData column; the datum points at its
    // NUL-terminated 64-byte buffer inside the pinned tuple image.
    Ok(unsafe {
        let p = d.as_usize() as *const u8;
        let mut len = 0usize;
        while len < NAMEDATALEN && *p.add(len) != 0 {
            len += 1;
        }
        core::str::from_utf8_unchecked(core::slice::from_raw_parts(p, len)).to_owned()
    })
}

// vacuum.h VACUUM_OPTION_* — the amparallelvacuumoptions vocabulary.
pub const VACUUM_OPTION_NO_PARALLEL: u8 = 0;
pub const VACUUM_OPTION_PARALLEL_BULKDEL: u8 = 1 << 0;
pub const VACUUM_OPTION_PARALLEL_COND_CLEANUP: u8 = 1 << 1;
pub const VACUUM_OPTION_PARALLEL_CLEANUP: u8 = 1 << 2;
pub const VACUUM_OPTION_MAX_VALID_VALUE: u8 = (1 << 3) - 1;

/// IndexAmRoutine.amparallelvacuumoptions per each AM's handler function.
pub fn amparallelvacuumoptions(kind: IndexAmKind) -> u8 {
    match kind {
        IndexAmKind::Btree | IndexAmKind::Gist | IndexAmKind::Spgist => {
            VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_COND_CLEANUP
        }
        IndexAmKind::Hash | IndexAmKind::Hnsw => VACUUM_OPTION_PARALLEL_BULKDEL,
        // gin and bloom: BULKDEL | CLEANUP (blutils.c blhandler).
        IndexAmKind::Gin | IndexAmKind::Bloom => {
            VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_CLEANUP
        }
        IndexAmKind::Brin => VACUUM_OPTION_PARALLEL_CLEANUP,
        #[allow(unreachable_patterns)]
        _ => VACUUM_OPTION_NO_PARALLEL,
    }
}

/// IndexAmRoutine.amusemaintenanceworkmem: true only for GIN.
pub fn amusemaintenanceworkmem(kind: IndexAmKind) -> bool {
    matches!(kind, IndexAmKind::Gin)
}

#[cold]
#[inline(never)]
fn unported_handler(amhandler: Oid) -> ! {
    panic!("unported: index AM handler function {amhandler} (IndexAmKind covers btree+brin; non-builtin handlers need pg_proc + extension loading)")
}

#[cold]
#[inline(never)]
fn unported_translate(amoid: Oid) -> ! {
    panic!("unported: amtranslatestrategy/amtranslatecmptype for non-btree AM {amoid}")
}

#[track_caller]
#[cold]
#[inline(never)]
fn am_lookup_failed(amoid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "cache lookup failed for access method {amoid}"
    )))
}

#[track_caller]
#[cold]
#[inline(never)]
fn not_index_am(name: String) -> Box<PgError> {
    Box::new(
        PgError::error(format!("access method \"{name}\" is not of type {}", "INDEX"))
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn no_handler(name: String) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "index access method \"{name}\" does not have a handler"
        ))
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
    )
}

#[cfg(test)]
mod tests;

// from_relam's lazy resolver: pg_am.amhandler -> builtin IndexAmRoutine
// (relcache/GetIndexAmRoutineByAmId path); None only when the pg_am row is
// missing, so the caller's loud panic stands.
fn resolve_index_am_kind(amoid: Oid) -> Option<IndexAmKind> {
    let tuple = SearchSysCache1(AMOID, SysCacheKey::Value(Datum::from_oid(amoid))).ok()??;
    let amhandler = SysCacheGetAttrNotNull(AMOID, &tuple, Anum_pg_am_amhandler)
        .map(|d| d.as_oid())
        .ok()?;
    ReleaseSysCache(tuple);
    if !matches!(
        amhandler,
        F_BTHANDLER | F_HASHHANDLER | F_GINHANDLER | F_GISTHANDLER | F_SPGHANDLER | F_BRINHANDLER
    ) {
        // Extension AM: identify by the handler proc's C symbol.
        let name = syscache_seams::pg_proc_proname::call(amhandler).ok()??;
        return match name.name_str() {
            b"hnswhandler" => Some(IndexAmKind::Hnsw),
            b"blhandler" => Some(IndexAmKind::Bloom),
            _ => None,
        };
    }
    Some(GetIndexAmRoutine(amhandler))
}

pub fn init_seams() {
    types_relscan::set_index_am_resolver(resolve_index_am_kind);
}
