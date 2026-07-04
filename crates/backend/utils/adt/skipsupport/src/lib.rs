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

    let mut sksup = match proc {
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
        // 6409 timestamp_skipsupport: DT_NOBEGIN/DT_NOEND are i64::MIN/MAX,
        // so the int8 kernels are exact.
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
        other => panic!(
            "unported: skip support proc {other} (skipsupport.c dispatch; by-ref types need an allocator seam)"
        ),
    };

    if reverse {
        core::mem::swap(&mut sksup.low_elem, &mut sksup.high_elem);
        core::mem::swap(&mut sksup.decrement, &mut sksup.increment);
    }
    Ok(Some(sksup))
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
}
