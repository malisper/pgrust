//! nbtpreprocesskeys.c, scalar arm: redundancy elimination + requiredness
//! marking. Array preprocessing (SAOP + skip arrays) and row compares are
//! phase 2 — detected up front and rejected loudly, never silently degraded.

use ::fmgr_core::{function_call2_coll, oid_function_call2_coll};
use ::types_error::{PgError, PgResult};
use ::types_nbtree::{BTCommuteStrategyNumber, BTScanOpaqueData};
use ::types_rel::Relation;
use ::types_scan::scankey::{
    ScanKeyData, BTEqualStrategyNumber, BTGreaterEqualStrategyNumber, BTGreaterStrategyNumber,
    BTLessEqualStrategyNumber, BTLessStrategyNumber,
    BTMaxStrategyNumber, SK_BT_DESC, SK_BT_INDOPTION_SHIFT, SK_BT_NULLS_FIRST, SK_BT_REQBKWD,
    SK_BT_REQFWD, SK_ISNULL, SK_ROW_HEADER, SK_SEARCHARRAY, SK_SEARCHNOTNULL, SK_SEARCHNULL,
};
use ::mcx::PgVec;

use crate::unported_phase2;

/// _bt_preprocess_keys; `input_keys` (scan->keyData) is mutated in place by
/// strategy fixup, as in C.
pub(crate) fn bt_preprocess_keys(
    rel: &Relation<'_>,
    so: &mut BTScanOpaqueData<'_>,
    input_keys: &mut [ScanKeyData],
) -> PgResult<()> {
    if so.numberOfKeys > 0 {
        return Ok(());
    }

    so.qual_ok = true;
    so.numberOfKeys = 0;

    let number_of_keys = input_keys.len();
    if number_of_keys < 1 {
        return Ok(()); // done if qual-less scan
    }

    reject_array_lanes(rel, input_keys);

    let indoption = &rel.rd_indoption;
    let inkeys = input_keys;

    if inkeys[0].sk_attno < 1 {
        return Err(keys_out_of_order());
    }

    so.keyData.clear();
    so.keyData.reserve(number_of_keys);

    if number_of_keys == 1 {
        if !bt_fix_scankey_strategy(&mut inkeys[0], indoption) {
            so.qual_ok = false;
        }
        so.keyData.push(inkeys[0].clone());
        so.numberOfKeys = 1;
        if inkeys[0].sk_attno == 1 {
            bt_mark_scankey_required(&mut so.keyData[0]);
        }
        return Ok(());
    }

    let mut new_number_of_keys: usize = 0;
    let mut number_of_equal_cols: usize = 0;

    let mut attno = 1;
    let mut xform: [Option<usize>; BTMaxStrategyNumber as usize] =
        [None; BTMaxStrategyNumber as usize];
    let mut redundant_key_kept = false;

    let mut i = 0usize;
    loop {
        if i < number_of_keys && !bt_fix_scankey_strategy(&mut inkeys[i], indoption) {
            so.qual_ok = false;
            return Ok(());
        }

        if i == number_of_keys || inkeys[i].sk_attno != attno {
            let prior_number_of_equal_cols = number_of_equal_cols;

            if i < number_of_keys && inkeys[i].sk_attno < attno {
                return Err(keys_out_of_order());
            }

            // whole qual contradictory).
            if let Some(eq_i) = xform[BTEqualStrategyNumber as usize - 1] {
                for j in (0..BTMaxStrategyNumber as usize).rev() {
                    if j == BTEqualStrategyNumber as usize - 1 {
                        continue;
                    }
                    let Some(chk_i) = xform[j] else { continue };

                    if inkeys[eq_i].sk_flags & SK_SEARCHNULL != 0 {
                        // IS NULL contradicts everything else.
                        so.qual_ok = false;
                        return Ok(());
                    }

                            match compare_scankey_args(rel, inkeys, chk_i, eq_i, chk_i)? {
                        Some(test_result) => {
                            if !test_result {
                                so.qual_ok = false;
                                return Ok(());
                            }
                            xform[j] = None; // redundant non-equality key
                        }
                        None => redundant_key_kept = true,
                    }
                }
                number_of_equal_cols += 1;
            }

            for (strict, loose) in [
                (BTLessStrategyNumber, BTLessEqualStrategyNumber),
                (BTGreaterStrategyNumber, BTGreaterEqualStrategyNumber),
            ] {
                let (si, li) = (strict as usize - 1, loose as usize - 1);
                if let (Some(st), Some(lo)) = (xform[si], xform[li]) {
                    match compare_scankey_args(rel, inkeys, lo, st, lo)? {
                        Some(test_result) => {
                            if test_result {
                                xform[li] = None;
                            } else {
                                xform[si] = None;
                            }
                        }
                        None => redundant_key_kept = true,
                    }
                }
            }

            // attributes had "=".
            for j in (0..BTMaxStrategyNumber as usize).rev() {
                if let Some(k) = xform[j] {
                    so.keyData.push(inkeys[k].clone());
                    new_number_of_keys += 1;
                    if prior_number_of_equal_cols == (attno - 1) as usize {
                        bt_mark_scankey_required(so.keyData.last_mut().expect("just pushed"));
                    }
                }
            }

            if i == number_of_keys {
                break;
            }

            attno = inkeys[i].sk_attno;
            xform = [None; BTMaxStrategyNumber as usize];
        }

        let j = inkeys[i].sk_strategy as usize - 1;

        match xform[j] {
            None => xform[j] = Some(i),
            Some(prev) => {
                match compare_scankey_args(rel, inkeys, i, i, prev)? {
                    Some(test_result) => {
                        if test_result {
                            xform[j] = Some(i);
                        } else if j == BTEqualStrategyNumber as usize - 1 {
                            so.qual_ok = false;
                            return Ok(());
                        }
                    }
                    None => {
                        so.keyData.push(inkeys[prev].clone());
                        new_number_of_keys += 1;
                        if number_of_equal_cols == (attno - 1) as usize {
                            bt_mark_scankey_required(
                                so.keyData.last_mut().expect("just pushed"),
                            );
                        }
                        xform[j] = Some(i);
                        redundant_key_kept = true;
                    }
                }
            }
        }
        i += 1;
    }

    so.numberOfKeys = new_number_of_keys as i32;

    // attribute and direction.
    if redundant_key_kept && so.qual_ok {
        bt_unmark_keys(so)?;
    }
    Ok(())
}

// Phase-2 gate: SAOP arrays and PG 18's skip-array backfill for omitted
// prefix attributes change scan semantics — panic, never silently degrade.
fn reject_array_lanes(rel: &Relation<'_>, input_keys: &[ScanKeyData]) {
    let mut max_attno = 0;
    for key in input_keys {
        if key.sk_flags & SK_SEARCHARRAY != 0 {
            unported_phase2("SK_SEARCHARRAY keys (_bt_preprocess_array_keys)");
        }
        max_attno = max_attno.max(key.sk_attno);
    }
    let mut has_eq = [false; ::types_core::INDEX_MAX_KEYS as usize + 1];
    for key in input_keys {
        if key.sk_strategy == BTEqualStrategyNumber || key.sk_flags & SK_SEARCHNULL != 0 {
            has_eq[key.sk_attno as usize] = true;
        }
    }
    let nkeyatts = rel.indnkeyatts().min(max_attno as i32);
    for attno in 1..nkeyatts.max(0) {
        if !has_eq[attno as usize] {
            unported_phase2("skip scan (_bt_num_array_keys backfill arrays)");
        }
    }
}

#[cold]
#[inline(never)]
fn keys_out_of_order() -> Box<PgError> {
    Box::new(PgError::error(
        "btree index keys must be ordered by attribute",
    ))
}

/// _bt_fix_scankey_strategy; false = unsatisfiable NULL qual.
fn bt_fix_scankey_strategy(skey: &mut ScanKeyData, indoption: &PgVec<'_, i16>) -> bool {
    let addflags = (indoption[skey.sk_attno as usize - 1] as i32) << SK_BT_INDOPTION_SHIFT;

    // match. IS NULL / IS NOT NULL keys keep going as =-like keys.
    if skey.sk_flags & SK_ISNULL != 0 {
        debug_assert!(skey.sk_flags & SK_ROW_HEADER == 0);
        skey.sk_flags |= addflags;

        if skey.sk_flags & SK_SEARCHNULL != 0 {
            skey.sk_strategy = BTEqualStrategyNumber;
            skey.sk_subtype = 0;
            skey.sk_collation = 0;
        } else if skey.sk_flags & SK_SEARCHNOTNULL != 0 {
            skey.sk_strategy = if skey.sk_flags & SK_BT_NULLS_FIRST != 0 {
                BTGreaterStrategyNumber
            } else {
                BTLessStrategyNumber
            };
            skey.sk_subtype = 0;
            skey.sk_collation = 0;
        } else {
            return false; // regular qual with NULL constant
        }
        return true;
    }

    if addflags & SK_BT_DESC != 0 && skey.sk_flags & SK_BT_DESC == 0 {
        skey.sk_strategy = BTCommuteStrategyNumber(skey.sk_strategy);
    }
    skey.sk_flags |= addflags;

    if skey.sk_flags & SK_ROW_HEADER != 0 {
        unported_phase2("row-comparison keys (_bt_fix_scankey_strategy)");
    }

    true
}

/// _bt_mark_scankey_required.
fn bt_mark_scankey_required(skey: &mut ScanKeyData) {
    let addflags = match skey.sk_strategy {
        BTLessStrategyNumber | BTLessEqualStrategyNumber => SK_BT_REQFWD,
        BTEqualStrategyNumber => SK_BT_REQFWD | SK_BT_REQBKWD,
        BTGreaterEqualStrategyNumber | BTGreaterStrategyNumber => SK_BT_REQBKWD,
        other => panic!("unrecognized StrategyNumber: {other}"),
    };
    skey.sk_flags |= addflags;
    debug_assert!(skey.sk_flags & SK_ROW_HEADER == 0, "row lane is phase 2");
}

/// _bt_compare_scankey_args, scalar arm: is "left op right" true? `None` when
/// the opfamily can't supply the cross-type comparison; op aliases an arg.
fn compare_scankey_args(
    rel: &Relation<'_>,
    keys: &[ScanKeyData],
    op: usize,
    leftarg: usize,
    rightarg: usize,
) -> PgResult<Option<bool>> {
    let (op, leftarg, rightarg) = (&keys[op], &keys[leftarg], &keys[rightarg]);
    debug_assert!((leftarg.sk_flags | rightarg.sk_flags) & SK_ROW_HEADER == 0);
    debug_assert!((leftarg.sk_flags | rightarg.sk_flags) & SK_SEARCHARRAY == 0);

    if (leftarg.sk_flags | rightarg.sk_flags) & SK_ISNULL != 0 {
        let leftnull = leftarg.sk_flags & SK_ISNULL != 0;
        let rightnull = rightarg.sk_flags & SK_ISNULL != 0;
        debug_assert!(!leftnull || leftarg.sk_flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL) != 0);
        debug_assert!(!rightnull || rightarg.sk_flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL) != 0);

        let mut strat = op.sk_strategy;
        if op.sk_flags & SK_BT_NULLS_FIRST != 0 {
            strat = BTCommuteStrategyNumber(strat);
        }
        let result = match strat {
            BTLessStrategyNumber => leftnull < rightnull,
            BTLessEqualStrategyNumber => leftnull <= rightnull,
            BTEqualStrategyNumber => leftnull == rightnull,
            BTGreaterEqualStrategyNumber => leftnull >= rightnull,
            BTGreaterStrategyNumber => leftnull > rightnull,
            other => panic!("unrecognized StrategyNumber: {other}"),
        };
        return Ok(Some(result));
    }

    debug_assert!(leftarg.sk_attno == rightarg.sk_attno);

    let opcintype = rel.rd_opcintype[leftarg.sk_attno as usize - 1];

    let lefttype = if leftarg.sk_subtype != 0 { leftarg.sk_subtype } else { opcintype };
    let righttype = if rightarg.sk_subtype != 0 { rightarg.sk_subtype } else { opcintype };
    let optype = if op.sk_subtype != 0 { op.sk_subtype } else { opcintype };

    if lefttype == opcintype && righttype == optype {
        // fmgr_info_copy clone stands in for C's persistent &op->sk_func.
        let mut func = op.sk_func.clone();
        let r = function_call2_coll(
            &mut func,
            op.sk_collation,
            leftarg.sk_argument,
            rightarg.sk_argument,
        )?;
        return Ok(Some(r.as_bool()));
    }

    let mut strat = op.sk_strategy;
    if op.sk_flags & SK_BT_DESC != 0 {
        strat = BTCommuteStrategyNumber(strat);
    }

    let cmp_op = lsyscache::get_opfamily_member(
        rel.rd_opfamily[leftarg.sk_attno as usize - 1],
        lefttype,
        righttype,
        strat as i16,
    )?;
    if cmp_op != 0 {
        let cmp_proc = lsyscache::get_opcode(cmp_op)?;
        if cmp_proc != 0 {
            let r = oid_function_call2_coll(
                cmp_proc,
                op.sk_collation,
                leftarg.sk_argument,
                rightarg.sk_argument,
            )?;
            return Ok(Some(r.as_bool()));
        }
    }

    Ok(None) // can't make the comparison
}

/// _bt_unmark_keys, scalar arm: one required key per attribute/direction.
fn bt_unmark_keys(so: &mut BTScanOpaqueData<'_>) -> PgResult<()> {
    let n = so.numberOfKeys as usize;
    let mcx = *so.keyData.allocator();
    let mut unmarkikey: PgVec<'_, bool> = ::mcx::vec_with_capacity_in(mcx, n)?;
    unmarkikey.resize(n, false);
    let mut nunmark = 0usize;

    let mut attno = so.keyData[0].sk_attno;
    let mut firsti = 0usize;
    let mut have_req_equals = false;
    let mut have_req_forward = false;
    let mut have_req_backward = false;

    for i in 0..n {
        let origkey = &so.keyData[i];

        if origkey.sk_attno != attno {
            attno = origkey.sk_attno;
            firsti = i;
            have_req_equals = false;
            have_req_forward = false;
            have_req_backward = false;
        }

        if have_req_equals {
            debug_assert!(origkey.sk_flags & SK_SEARCHNULL == 0);
            unmarkikey[i] = true;
            nunmark += 1;
            continue;
        }
        if origkey.sk_flags & SK_BT_REQFWD != 0 && origkey.sk_flags & SK_BT_REQBKWD != 0 {
            debug_assert!(origkey.sk_strategy == BTEqualStrategyNumber);
            have_req_equals = true;
            for item in unmarkikey[firsti..i].iter_mut() {
                if !*item {
                    *item = true;
                    nunmark += 1;
                }
            }
            continue;
        }

        if origkey.sk_flags & SK_BT_REQFWD != 0 && !have_req_forward {
            have_req_forward = true;
            continue;
        }
        if origkey.sk_flags & SK_BT_REQBKWD != 0 && !have_req_backward {
            have_req_backward = true;
            continue;
        }

        unmarkikey[i] = true;
        nunmark += 1;
    }

    debug_assert!(nunmark > 0, "only called when a redundant key was kept");

    // ScanKeyData is droppy (sk_func.fn_extra): plain reserve, not the
    // !needs_drop arena helper.
    let mut kept: PgVec<'_, ScanKeyData> = PgVec::new_in(mcx);
    kept.reserve(n - nunmark);
    let mut unmarked: PgVec<'_, ScanKeyData> = PgVec::new_in(mcx);
    unmarked.reserve(nunmark);
    for (i, key) in so.keyData.iter().enumerate() {
        if !unmarkikey[i] {
            kept.push(key.clone());
        } else {
            debug_assert!(
                key.sk_flags & SK_ISNULL == 0
                    || key.sk_flags & (SK_BT_REQFWD | SK_BT_REQBKWD) == 0
            );
            let mut key = key.clone();
            key.sk_flags &= !(SK_BT_REQFWD | SK_BT_REQBKWD);
            unmarked.push(key);
        }
    }
    so.keyData.clear();
    so.keyData.extend(kept.into_iter());
    so.keyData.extend(unmarked.into_iter());
    Ok(())
}
