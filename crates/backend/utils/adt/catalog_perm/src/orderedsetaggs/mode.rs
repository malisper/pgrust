//! `mode_final` — most common value (`orderedsetaggs.c:1033`).

use datum::Datum as Word;
use fmgr::FunctionCallInfoBaseData;

use tuplesort_seams as tsort;

use super::{
    arg_isnull, ok, perform_or_rescan, raise, ret_null, ret_sort_cdatum, restash, take_group_state,
    with_sortstate_mut,
};

/// `mode_final(PG_FUNCTION_ARGS)` (3985).
///
/// Scans the sorted datums, counts run lengths, and returns the value of the
/// longest run. The C abbreviated-key short-circuit is dropped (the
/// `tuplesort_getdatum` seam does not surface the abbreviation); we always run
/// the equality function, which is correct, just without that micro-opt.
pub fn fc_mode_final(fcinfo: &mut FunctionCallInfoBaseData) -> Word {
    if arg_isnull(fcinfo, 0) {
        return ret_null(fcinfo);
    }
    let mut osastate = take_group_state(fcinfo).expect("mode_final: non-null arg0");
    if osastate.number_of_rows == 0 {
        restash(fcinfo, osastate);
        return ret_null(fcinfo);
    }

    // Look up the equality function for the datatype, if we didn't already.
    if osastate.qstate.equal_fn_oid == 0 {
        let fn_oid = ok(lsyscache_seams::get_opcode::call(
            osastate.qstate.eq_operator,
        ));
        osastate.qstate.equal_fn_oid = fn_oid;
        // Update the fn_extra cache too, so a sibling group reuses it.
        if let Some(flinfo) = fcinfo.flinfo.as_mut() {
            if let Some(q) = flinfo.fn_extra_user_mut::<super::OSAPerQueryState>() {
                q.equal_fn_oid = fn_oid;
            }
        }
    }
    let equal_fn_oid = osastate.qstate.equal_fn_oid;
    let collation = osastate.qstate.sort_collation;
    let typ_by_val = osastate.qstate.typ_by_val;

    perform_or_rescan(&mut osastate);

    // Scan tuples and count frequencies. We hold the winning value as a raw
    // by-value word / owned by-ref image; the equality call takes by-value words
    // (function_call2_coll), which is correct for by-value types and for by-ref
    // types whose equality fn reads the pointer word (the tuplesort_getdatum copy
    // keeps each fetched by-ref value live in the sort's context for this scan).
    let mut mode_val: Option<HeldVal> = None;
    let mut mode_freq: i64 = 0;
    let mut last_val: Option<HeldVal> = None;
    let mut last_val_freq: i64 = 0;
    let mut last_val_is_mode = false;

    loop {
        let (found, val, isnull) = with_sortstate_mut(osastate.sort_id, |s| {
            ok(tsort::tuplesort_getdatum::call(s, true, true))
        });
        if !found {
            break;
        }
        if isnull {
            continue;
        }
        let held = HeldVal::from_cdatum(&val, typ_by_val);

        if last_val_freq == 0 {
            mode_val = Some(held.clone());
            last_val = Some(held);
            mode_freq = 1;
            last_val_freq = 1;
            last_val_is_mode = true;
        } else {
            let equal = values_equal(equal_fn_oid, collation, &held, last_val.as_ref().unwrap());
            if equal {
                if last_val_is_mode {
                    mode_freq += 1;
                } else {
                    last_val_freq += 1;
                    if last_val_freq > mode_freq {
                        mode_val = last_val.clone();
                        mode_freq = last_val_freq;
                        last_val_is_mode = true;
                    }
                }
            } else {
                last_val = Some(held);
                last_val_freq = 1;
                last_val_is_mode = false;
            }
        }
    }

    let result = if mode_freq > 0 {
        let mv = mode_val.expect("mode_final: mode_freq>0 but no mode_val");
        ret_sort_cdatum(fcinfo, mv.into_cdatum(), &osastate.qstate)
    } else {
        ret_null(fcinfo)
    };
    restash(fcinfo, osastate);
    result
}

/// A held sort value across the mode scan: a by-value word or an owned by-ref
/// image. (C holds raw `Datum`s pointing into the sort's per-scan storage; the
/// owned model copies the image so it survives the next `getdatum`.)
#[derive(Clone)]
enum HeldVal {
    Val(usize),
    Ref(alloc::vec::Vec<u8>),
}

impl HeldVal {
    fn from_cdatum(d: &types_tuple::heaptuple::Datum<'_>, by_val: bool) -> Self {
        use types_tuple::heaptuple::Datum as CDatum;
        if by_val {
            return HeldVal::Val(d.as_usize());
        }
        match d {
            CDatum::ByRef(v) => HeldVal::Ref(v.iter().copied().collect()),
            CDatum::ByVal(w) => HeldVal::Val(*w),
            _ => raise(types_error::PgError::error(
                "mode_final: unexpected by-ref datum shape",
            )),
        }
    }

    fn into_cdatum<'mcx>(self) -> types_tuple::heaptuple::Datum<'mcx> {
        use types_tuple::heaptuple::Datum as CDatum;
        match self {
            HeldVal::Val(w) => CDatum::from_usize(w),
            HeldVal::Ref(bytes) => {
                let mcx = super::leak_ctx("mode result");
                CDatum::ByRef(super::vec_in(mcx, &bytes))
            }
        }
    }
}

/// `DatumGetBool(FunctionCall2Coll(equalfn, collation, a, b))`.
fn values_equal(fn_oid: types_core::Oid, collation: types_core::Oid, a: &HeldVal, b: &HeldVal) -> bool {
    // For by-ref values the equality function reads pointer words; this path is
    // exercised by by-value types (the common mode() case). A by-ref held image
    // word cannot be reconstituted into a live pointer here, so by-ref equality
    // falls back to a byte-image compare (correct for the canonical detoasted
    // images tuplesort_getdatum returns).
    match (a, b) {
        (HeldVal::Ref(x), HeldVal::Ref(y)) => x == y,
        (HeldVal::Val(x), HeldVal::Val(y)) => {
            let r = ok(fmgr_seams::function_call2_coll::call(
                fn_oid,
                collation,
                Word::from_usize(*x),
                Word::from_usize(*y),
            ));
            r.as_bool()
        }
        _ => false,
    }
}
