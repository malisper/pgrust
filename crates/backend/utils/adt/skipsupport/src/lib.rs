//! skipsupport.c: PrepareSkipSupportFromOpclass, plus the amproc-6 dispatch
//! (C reaches the opclass function through fmgr; the SkipSupportData callback
//! shape doesn't cross our fmgr boundary, so we dispatch on the proc OID).

use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_nbtree::{SkipSupportData, BTSKIPSUPPORT_PROC};

pub fn prepare_skip_support_from_opclass(
    opfamily: Oid,
    opcintype: Oid,
    reverse: bool,
) -> PgResult<Option<SkipSupportData>> {
    let proc = lsyscache::get_opfamily_proc(
        opfamily,
        opcintype,
        opcintype,
        BTSKIPSUPPORT_PROC as i16,
    )?;
    if proc == 0 {
        return Ok(None);
    }

    let mut sksup = match skip_support_for_proc(proc)? {
        Some(s) => s,
        None => return Ok(None),
    };

    if reverse {
        core::mem::swap(&mut sksup.low_elem, &mut sksup.high_elem);
        core::mem::swap(&mut sksup.decrement, &mut sksup.increment);
    }
    Ok(Some(sksup))
}

// skipsupport.c:83 OidFunctionCall1(skipSupportFunction, sksup): C reaches
// whatever pg_proc row the opclass's amproc 6 names.  The catalog rows are the
// builtin OIDs below; a user-declared `LANGUAGE internal` alias of one of them
// (CREATE FUNCTION f(internal) ... AS 'btint4skipsupport', then FUNCTION 6 f
// in a CREATE OPERATOR CLASS) carries a fresh OID and is resolved through its
// prosrc, exactly as fmgr resolves the internal function C would call.  Any
// other prosrc has no engine here (pgrust has no C extension loader), so it
// is a typed refusal rather than a panic.
fn skip_support_for_proc(proc: Oid) -> PgResult<Option<SkipSupportData>> {
    if let Some(s) = skip_support_for_builtin(proc) {
        return Ok(s);
    }
    let ctx = ::mcx::MemoryContext::new("skipsupport prosrc");
    let Some(prosrc) = syscache_seams::lookup_pg_proc_prosrc::call(ctx.mcx(), proc)? else {
        // C's OidFunctionCall1 -> fmgr_info elog(ERROR, "cache lookup failed
        // for function %u").
        return Err(Box::new(::types_error::PgError::error(format!(
            "cache lookup failed for function {proc}"
        ))));
    };
    let builtin_oid = match prosrc.as_str() {
        "btint2skipsupport" => 6402,
        "btint4skipsupport" => 6403,
        "btint8skipsupport" => 6404,
        "btoidskipsupport" => 6405,
        "btcharskipsupport" => 6406,
        "date_skipsupport" => 6407,
        "btboolskipsupport" => 6408,
        "timestamp_skipsupport" => 6409,
        "uuid_skipsupport" => 6410,
        other => {
            return Err(Box::new(
                ::types_error::PgError::error(format!(
                    "skip support function \"{other}\" is not supported"
                ))
                .with_sqlstate(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
    };
    Ok(skip_support_for_builtin(builtin_oid)
        .expect("every prosrc name above maps to a builtin skip support OID"))
}

/// `Some(Some(data))` for a ported builtin, `Some(None)` for a builtin whose
/// by-ref bounds are not ported (skip scan degrades to a plain scan, results
/// identical), `None` when `proc` is not one of the builtin OIDs.
fn skip_support_for_builtin(proc: Oid) -> Option<Option<SkipSupportData>> {
    Some(Some(match proc {
        6402 => SkipSupportData {
            low_elem: Datum::from_i16(i16::MIN),
            high_elem: Datum::from_i16(i16::MAX),
            decrement: nbt_compare::int2_decrement,
            increment: nbt_compare::int2_increment,
        },
        6403 => SkipSupportData {
            low_elem: Datum::from_i32(i32::MIN),
            high_elem: Datum::from_i32(i32::MAX),
            decrement: nbt_compare::int4_decrement,
            increment: nbt_compare::int4_increment,
        },
        // 6409 timestamp_skipsupport: DT_NOBEGIN/DT_NOEND are i64::MIN/MAX.
        6404 | 6409 => SkipSupportData {
            low_elem: Datum::from_i64(i64::MIN),
            high_elem: Datum::from_i64(i64::MAX),
            decrement: nbt_compare::int8_decrement,
            increment: nbt_compare::int8_increment,
        },
        6405 => SkipSupportData {
            low_elem: Datum::from_u32(0),
            high_elem: Datum::from_u32(u32::MAX),
            decrement: nbt_compare::oid_decrement,
            increment: nbt_compare::oid_increment,
        },
        6406 => SkipSupportData {
            low_elem: Datum::from_u8(0),
            high_elem: Datum::from_u8(u8::MAX),
            decrement: nbt_compare::char_decrement,
            increment: nbt_compare::char_increment,
        },
        6407 => SkipSupportData {
            low_elem: Datum::from_i32(adt_date::DATEVAL_NOBEGIN),
            high_elem: Datum::from_i32(adt_date::DATEVAL_NOEND),
            decrement: adt_date::date_decrement,
            increment: adt_date::date_increment,
        },
        6408 => SkipSupportData {
            low_elem: Datum::from_bool(false),
            high_elem: Datum::from_bool(true),
            decrement: nbt_compare::bool_decrement,
            increment: nbt_compare::bool_increment,
        },
        // uuid_skipsupport: by-ref bounds need an allocator seam; no skip
        // support means nbtree runs the scan without skipping.
        6410 => return Some(None),
        _ => return None,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int4_incdec_edges() {
        let mut flow = false;
        assert_eq!(nbt_compare::int4_increment(Datum::from_i32(41), &mut flow).as_i32(), 42);
        assert!(!flow);
        nbt_compare::int4_increment(Datum::from_i32(i32::MAX), &mut flow);
        assert!(flow);
        assert_eq!(
            nbt_compare::int4_decrement(Datum::from_i32(i32::MIN + 1), &mut flow).as_i32(),
            i32::MIN
        );
        assert!(!flow);
        nbt_compare::int4_decrement(Datum::from_i32(i32::MIN), &mut flow);
        assert!(flow);
    }

    #[test]
    fn date_matches_c_sentinels() {
        let mut flow = false;
        adt_date::date_increment(Datum::from_i32(adt_date::DATEVAL_NOEND), &mut flow);
        assert!(flow);
        adt_date::date_decrement(Datum::from_i32(adt_date::DATEVAL_NOBEGIN), &mut flow);
        assert!(flow);
    }

    #[test]
    fn uuid_skipsupport_degrades_without_allocator() {
        assert!(skip_support_for_proc(6403).unwrap().is_some());
        assert!(skip_support_for_proc(6410).unwrap().is_none());
    }

    // audit-18.6 a186-candidate-fp-adt-b5-3539a9a35e09c0fc3873-1:
    // skipsupport.c:83 OidFunctionCall1 reaches whatever pg_proc row the
    // opclass's amproc 6 names.  CREATE FUNCTION f(internal) ... LANGUAGE
    // internal AS 'btint4skipsupport' + CREATE OPERATOR CLASS ... FUNCTION 6 f
    // gives that builtin a fresh OID; a skip scan over such an index must
    // resolve it (through prosrc), and an unknown prosrc is a typed refusal
    // -- never a panic.
    fn install_alias_seams() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            syscache_seams::lookup_pg_amproc::set(|opfamily, _lt, _rt, procnum| {
                assert_eq!(procnum, BTSKIPSUPPORT_PROC as i16);
                Ok(match opfamily {
                    1000 => 16430,
                    1001 => 16431,
                    1002 => 16432,
                    1003 => 16499,
                    _ => 0,
                })
            });
            syscache_seams::lookup_pg_proc_prosrc::set(|mcx, funcid| {
                Ok(match funcid {
                    16430 => Some(::mcx::PgString::from_str_in("btint4skipsupport", mcx)?),
                    16431 => Some(::mcx::PgString::from_str_in("my_skip", mcx)?),
                    16432 => Some(::mcx::PgString::from_str_in("uuid_skipsupport", mcx)?),
                    _ => None,
                })
            });
        });
    }

    #[test]
    fn user_alias_of_builtin_skip_support_resolves() {
        install_alias_seams();
        let s = prepare_skip_support_from_opclass(1000, 23, false)
            .unwrap()
            .expect("btint4skipsupport alias");
        assert_eq!(s.low_elem.as_i32(), i32::MIN);
        assert_eq!(s.high_elem.as_i32(), i32::MAX);
        let s = prepare_skip_support_from_opclass(1000, 23, true).unwrap().unwrap();
        assert_eq!(s.low_elem.as_i32(), i32::MAX);
        // uuid alias: unported by-ref bounds degrade to no skip support.
        assert!(prepare_skip_support_from_opclass(1002, 2950, false).unwrap().is_none());
        // no amproc 6 at all.
        assert!(prepare_skip_support_from_opclass(1, 23, false).unwrap().is_none());
    }

    #[test]
    fn unknown_skip_support_prosrc_is_typed_refusal() {
        install_alias_seams();
        let expect_err = |r: PgResult<Option<SkipSupportData>>| match r {
            Err(e) => e,
            Ok(_) => panic!("expected an error"),
        };
        let err = expect_err(prepare_skip_support_from_opclass(1001, 23, false));
        assert_eq!(err.sqlstate(), ::types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
        assert_eq!(err.message(), "skip support function \"my_skip\" is not supported");
        let err = expect_err(prepare_skip_support_from_opclass(1003, 23, false));
        assert_eq!(err.message(), "cache lookup failed for function 16499");
    }
}
