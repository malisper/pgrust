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
        other => unported_handler(other),
    }
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
    let _ = opfamily;
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

pub fn amvalidate(opclassoid: Oid) -> PgResult<bool> {
    let shape = syscache_seams::lookup_pg_opclass_shape::call(opclassoid)?
        .unwrap_or_else(|| panic!("cache lookup failed for operator class {opclassoid}"));
    let kind = GetIndexAmRoutineByAmId(shape.opcmethod, false)?.expect("noerror=false");
    match kind {
        IndexAmKind::Btree => nbt_validate::btvalidate(opclassoid),
        other => panic!("unported: amvalidate for index AM {other:?} (hashvalidate lane)"),
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
        // hashadjustmembers exists in C; loud until the hash opclass lane.
        other => panic!("unported: amadjustmembers for index AM {other:?}"),
    }
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

#[cold]
#[inline(never)]
fn unported_handler(amhandler: Oid) -> ! {
    panic!("unported: index AM handler function {amhandler} (IndexAmKind covers btree only; non-builtin handlers need pg_proc + extension loading)")
}

#[cold]
#[inline(never)]
fn unported_translate(amoid: Oid) -> ! {
    panic!("unported: amtranslatestrategy/amtranslatecmptype for non-btree AM {amoid}")
}

#[cold]
#[inline(never)]
fn am_lookup_failed(amoid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "cache lookup failed for access method {amoid}"
    )))
}

#[cold]
#[inline(never)]
fn not_index_am(name: String) -> Box<PgError> {
    Box::new(
        PgError::error(format!("access method \"{name}\" is not of type {}", "INDEX"))
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
    )
}

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
