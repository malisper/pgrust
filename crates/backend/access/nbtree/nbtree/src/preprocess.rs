//! nbtpreprocesskeys.c: redundancy elimination + requiredness marking, the
//! SAOP array arms (deconstruct/sort/merge/shrink), and skip-array generation
//! (PG 18 skip scan), and the row-comparison arms.

use ::arrayfuncs::foundation as arrfn;
use ::datum::Datum;
use ::fmgr_core::{fmgr_info, function_call2_coll, oid_function_call2_coll};
use ::mcx::{Mcx, PgVec};
use ::types_core::INDEX_MAX_KEYS;
use ::types_error::{PgError, PgResult};
use ::types_fmgr::FmgrInfo;
use ::types_nbtree::{BTArrayKeyInfo, BTCommuteStrategyNumber, BTScanOpaqueData, BTORDER_PROC};
use ::types_rel::Relation;
use ::types_scan::scankey::{
    ScanKeyData, StrategyNumber, BTEqualStrategyNumber, BTGreaterEqualStrategyNumber,
    BTGreaterStrategyNumber, BTLessEqualStrategyNumber, BTLessStrategyNumber,
    BTMaxStrategyNumber, SK_BT_DESC, SK_BT_INDOPTION_SHIFT, SK_BT_NULLS_FIRST, SK_BT_REQBKWD,
    SK_BT_REQFWD, SK_BT_SKIP, SK_ISNULL, SK_ROW_END, SK_ROW_HEADER, SK_ROW_MEMBER,
    SK_SEARCHARRAY, SK_SEARCHNOTNULL, SK_SEARCHNULL,
};


const INVERT_COMPARE_RESULT: fn(i32) -> i32 = |r| if r < 0 { 1 } else { -r };

#[derive(Clone, Copy)]
struct XformEntry {
    inkeyi: usize,
    arrayidx: i32,
}

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

    if input_keys.is_empty() {
        return Ok(()); // done if qual-less scan
    }

    let mcx = *so.keyData.allocator();
    let mut array_key_data = bt_preprocess_array_keys(mcx, rel, so, input_keys)?;
    if !so.qual_ok {
        return Ok(()); // unmatchable array, so give up
    }

    let indoption = &rel.rd_indoption;
    let have_arrays = array_key_data.is_some();
    let inkeys: &mut [ScanKeyData] = match array_key_data.as_mut() {
        Some(v) => v.as_mut_slice(),
        None => input_keys,
    };
    let number_of_keys = inkeys.len();
    let mut key_data_map: PgVec<'_, i32> = PgVec::new_in(mcx);
    if have_arrays {
        key_data_map.reserve(number_of_keys);
    }

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
        // C skips _bt_preprocess_array_keys_final on this fast path (misses
        // only the single-value array transformation).
        debug_assert!(!have_arrays || so.keyData[0].sk_flags & SK_SEARCHARRAY != 0);
        return Ok(());
    }

    let mut number_of_equal_cols: usize = 0;

    let mut attno = 1;
    let mut xform: [Option<XformEntry>; BTMaxStrategyNumber as usize] =
        [None; BTMaxStrategyNumber as usize];
    let mut redundant_key_kept = false;
    let mut arrayidx: i32 = 0;

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

            if let Some(eq) = xform[BTEqualStrategyNumber as usize - 1] {
                for j in (0..BTMaxStrategyNumber as usize).rev() {
                    if j == BTEqualStrategyNumber as usize - 1 {
                        continue;
                    }
                    let Some(chk) = xform[j] else { continue };

                    if inkeys[eq.inkeyi].sk_flags & SK_SEARCHNULL != 0 {
                        // IS NULL contradicts everything else.
                        so.qual_ok = false;
                        return Ok(());
                    }

                    let eq_is_array =
                        have_arrays && inkeys[eq.inkeyi].sk_flags & SK_SEARCHARRAY != 0;
                    match compare_scankey_args(
                        rel,
                        so,
                        inkeys,
                        chk.inkeyi,
                        eq.inkeyi,
                        chk.inkeyi,
                        if eq_is_array { Some((eq.arrayidx - 1, eq.inkeyi)) } else { None },
                    )? {
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
                    match compare_scankey_args(
                        rel, so, inkeys, lo.inkeyi, st.inkeyi, lo.inkeyi, None,
                    )? {
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

            for j in (0..BTMaxStrategyNumber as usize).rev() {
                if let Some(k) = xform[j] {
                    so.keyData.push(inkeys[k.inkeyi].clone());
                    if have_arrays {
                        key_data_map.push(k.inkeyi as i32);
                    }
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

        if inkeys[i].sk_strategy == BTEqualStrategyNumber
            && inkeys[i].sk_flags & SK_SEARCHARRAY != 0
        {
            debug_assert!(have_arrays);
            arrayidx += 1;
        }

        match xform[j] {
            None => xform[j] = Some(XformEntry { inkeyi: i, arrayidx }),
            Some(prev) => {
                // Pass whichever of the pair is the array (both-arrays means
                // preprocessing couldn't merge: compare returns None below).
                let mut array_ref: Option<(i32, usize)> = None;
                if j == BTEqualStrategyNumber as usize - 1 && have_arrays {
                    if inkeys[i].sk_flags & SK_SEARCHARRAY != 0 {
                        array_ref = Some((arrayidx - 1, i));
                    } else if inkeys[prev.inkeyi].sk_flags & SK_SEARCHARRAY != 0 {
                        array_ref = Some((prev.arrayidx - 1, prev.inkeyi));
                    }
                }
                let both_arrays = j == BTEqualStrategyNumber as usize - 1
                    && inkeys[i].sk_flags & SK_SEARCHARRAY != 0
                    && inkeys[prev.inkeyi].sk_flags & SK_SEARCHARRAY != 0;
                let cmp = if both_arrays {
                    None
                } else {
                    compare_scankey_args(rel, so, inkeys, i, i, prev.inkeyi, array_ref)?
                };
                match cmp {
                    Some(test_result) => {
                        if test_result {
                            if j != BTEqualStrategyNumber as usize - 1
                                || inkeys[prev.inkeyi].sk_flags & SK_SEARCHARRAY == 0
                            {
                                xform[j] = Some(XformEntry { inkeyi: i, arrayidx });
                            } else {
                                // Keep the old key: it's the array that made
                                // the new key redundant.
                                debug_assert!(inkeys[i].sk_flags & SK_SEARCHARRAY == 0);
                            }
                        } else if j == BTEqualStrategyNumber as usize - 1 {
                            so.qual_ok = false;
                            return Ok(());
                        }
                    }
                    None => {
                        so.keyData.push(inkeys[prev.inkeyi].clone());
                        if have_arrays {
                            key_data_map.push(prev.inkeyi as i32);
                        }
                        if number_of_equal_cols == (attno - 1) as usize {
                            bt_mark_scankey_required(
                                so.keyData.last_mut().expect("just pushed"),
                            );
                        }
                        xform[j] = Some(XformEntry { inkeyi: i, arrayidx });
                        redundant_key_kept = true;
                    }
                }
            }
        }
        i += 1;
    }

    so.numberOfKeys = so.keyData.len() as i32;

    if have_arrays {
        bt_preprocess_array_keys_final(rel, so, &key_data_map)?;
    }

    if redundant_key_kept && so.qual_ok {
        bt_unmark_keys(so)?;
    }
    Ok(())
}

#[track_caller]
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

    // Fix row member flags and strategies the same way (the subkey array is
    // the executor-owned allocation behind sk_argument; C scribbles on it
    // too, and the fixup is idempotent across rescans).
    if skey.sk_flags & SK_ROW_HEADER != 0 {
        // SAFETY: SK_ROW_HEADER contract (scankey.rs): sk_argument is the
        // live SK_ROW_END-terminated subkey array, disjoint from *skey.
        unsafe {
            let mut subkey = skey.sk_argument.as_usize() as *mut ScanKeyData;
            if (*subkey).sk_flags & SK_ISNULL != 0 {
                // First row member is NULL: RowCompare is unsatisfiable.
                debug_assert!((*subkey).sk_flags & SK_ROW_MEMBER != 0);
                return false;
            }
            loop {
                debug_assert!((*subkey).sk_flags & SK_ROW_MEMBER != 0);
                let addflags = (indoption[(*subkey).sk_attno as usize - 1] as i32)
                    << SK_BT_INDOPTION_SHIFT;
                if addflags & SK_BT_DESC != 0 && (*subkey).sk_flags & SK_BT_DESC == 0 {
                    (*subkey).sk_strategy = BTCommuteStrategyNumber((*subkey).sk_strategy);
                }
                (*subkey).sk_flags |= addflags;
                if (*subkey).sk_flags & SK_ROW_END != 0 {
                    break;
                }
                subkey = subkey.add(1);
            }
        }
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

    // Row members stay required only while adjacent to the header's column
    // and sorted in the same direction; C scribbles on the shared subkey
    // array (idempotent across rescans).
    if skey.sk_flags & SK_ROW_HEADER != 0 {
        // SAFETY: SK_ROW_HEADER contract (scankey.rs).
        unsafe {
            let mut subkey = skey.sk_argument.as_usize() as *mut ScanKeyData;
            let mut attno = skey.sk_attno;
            debug_assert!((*subkey).sk_attno == attno);
            debug_assert!((*subkey).sk_strategy == skey.sk_strategy);
            loop {
                debug_assert!((*subkey).sk_flags & SK_ROW_MEMBER != 0);
                if (*subkey).sk_attno != attno {
                    break; // non-adjacent key, so not required
                }
                if (*subkey).sk_strategy != skey.sk_strategy {
                    break; // wrong direction, so not required
                }
                (*subkey).sk_flags |= addflags;
                if (*subkey).sk_flags & SK_ROW_END != 0 {
                    break;
                }
                subkey = subkey.add(1);
                attno += 1;
            }
        }
    }
}

/// _bt_compare_scankey_args: is "left op right" true? `None` when the
/// opfamily can't supply the cross-type comparison; op aliases an arg.
/// `array` = (so->arrayKeys index, orderProcs index) when exactly one of the
/// args is an equality-type array key.
fn compare_scankey_args(
    rel: &Relation<'_>,
    so: &mut BTScanOpaqueData<'_>,
    keys: &[ScanKeyData],
    op: usize,
    leftarg: usize,
    rightarg: usize,
    array: Option<(i32, usize)>,
) -> PgResult<Option<bool>> {
    let (op, left_i, right_i) = (&keys[op], leftarg, rightarg);
    let (leftarg, rightarg) = (&keys[left_i], &keys[right_i]);

    if (leftarg.sk_flags | rightarg.sk_flags) & SK_ISNULL != 0 {
        if (leftarg.sk_flags | rightarg.sk_flags) & SK_BT_SKIP != 0 {
            // IS NOT NULL key vs skip array: the key is redundant once the
            // array is known to exclude its NULL element.
            debug_assert!((leftarg.sk_flags | rightarg.sk_flags) & SK_SEARCHNULL == 0);
            debug_assert!((leftarg.sk_flags | rightarg.sk_flags) & SK_SEARCHNOTNULL != 0);
            let (array_i, _) = array.expect("skip array key carries its array");
            let arr = &mut so.arrayKeys[array_i as usize];
            debug_assert!(arr.num_elems == -1);
            arr.null_elem = false;
            return Ok(Some(true));
        }
        return compare_scankey_args_scalar(rel, op, leftarg, rightarg);
    }

    // Redundancy involving a row compare key is undetermined (C returns
    // "cannot compare" and both keys are kept).
    if (leftarg.sk_flags | rightarg.sk_flags) & SK_ROW_HEADER != 0 {
        debug_assert!((leftarg.sk_flags | rightarg.sk_flags) & SK_BT_SKIP == 0);
        return Ok(None);
    }

    if let Some((array_i, orderproc_i)) = array {
        let leftarray = leftarg.sk_flags & SK_SEARCHARRAY != 0
            && leftarg.sk_strategy == BTEqualStrategyNumber;
        let rightarray = rightarg.sk_flags & SK_SEARCHARRAY != 0
            && rightarg.sk_strategy == BTEqualStrategyNumber;
        debug_assert!(!(leftarray && rightarray), "caller handles both-arrays");
        if leftarray || rightarray {
            let (arraysk_i, skey_i) = if leftarray { (left_i, right_i) } else { (right_i, left_i) };
            if so.arrayKeys[array_i as usize].num_elems != -1 {
                return bt_saoparray_shrink(rel, so, arraysk_i, skey_i, keys, array_i, orderproc_i);
            }
            return bt_skiparray_shrink(rel, so, &keys[skey_i], array_i);
        }
    }

    compare_scankey_args_scalar(rel, op, leftarg, rightarg)
}

// _bt_compare_scankey_args, scalar tail: no array involvement (op may alias
// either arg, so it's a separate borrow).
fn compare_scankey_args_scalar(
    rel: &Relation<'_>,
    op: &ScanKeyData,
    leftarg: &ScanKeyData,
    rightarg: &ScanKeyData,
) -> PgResult<Option<bool>> {
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

/// _bt_saoparray_shrink: eliminate array elements contradicted by the scalar
/// key at `skey_i`; Some(qual_ok) on success, None when the opfamily can't
/// supply the cross-type ORDER proc.
fn bt_saoparray_shrink(
    rel: &Relation<'_>,
    so: &mut BTScanOpaqueData<'_>,
    arraysk_i: usize,
    skey_i: usize,
    keys: &[ScanKeyData],
    array_i: i32,
    orderproc_i: usize,
) -> PgResult<Option<bool>> {
    let arraysk = &keys[arraysk_i];
    let skey = &keys[skey_i];
    debug_assert!(arraysk.sk_attno == skey.sk_attno);
    debug_assert!(
        arraysk.sk_flags & SK_SEARCHARRAY != 0
            && arraysk.sk_strategy == BTEqualStrategyNumber
    );
    debug_assert!(skey.sk_flags & (SK_ISNULL | SK_ROW_HEADER) == 0);

    let opcintype = rel.rd_opcintype[arraysk.sk_attno as usize - 1];
    let mut crosstype_proc: Option<FmgrInfo> = None;

    if skey.sk_subtype != opcintype && skey.sk_subtype != 0 {
        let arraysk_elemtype = if arraysk.sk_subtype != 0 { arraysk.sk_subtype } else { opcintype };
        let cmp_proc = lsyscache::get_opfamily_proc(
            rel.rd_opfamily[arraysk.sk_attno as usize - 1],
            skey.sk_subtype,
            arraysk_elemtype,
            BTORDER_PROC as i16,
        )?;
        if cmp_proc == 0 {
            return Ok(None); // can't make the comparison
        }
        crosstype_proc = Some(fmgr_info(cmp_proc)?);
    }

    let BTScanOpaqueData { arrayKeys, orderProcs, .. } = so;
    let array = &mut arrayKeys[array_i as usize];
    debug_assert!(array.num_elems > 0);
    let orderproc = match crosstype_proc.as_mut() {
        Some(p) => p,
        None => &mut orderProcs[orderproc_i],
    };

    let mut cmpresult: i32 = 0;
    let mut frame = crate::fcframe::OrderProcFrame::new();
    let mut matchelem = bt_binsrch_array_skey_raw(
        &mut frame,
        orderproc,
        arraysk.sk_collation,
        arraysk.sk_flags,
        skey.sk_argument,
        false,
        array,
        &mut cmpresult,
    )?;

    let new_nelems: i32;
    match skey.sk_strategy {
        BTLessStrategyNumber | BTLessEqualStrategyNumber => {
            let cmpexact = if skey.sk_strategy == BTLessStrategyNumber { 1 } else { 0 };
            if cmpresult >= cmpexact {
                matchelem += 1;
            }
            new_nelems = matchelem;
        }
        BTEqualStrategyNumber => {
            if cmpresult != 0 {
                new_nelems = 0;
            } else {
                array.elem_values[0] = array.elem_values[matchelem as usize];
                new_nelems = 1;
            }
        }
        BTGreaterEqualStrategyNumber | BTGreaterStrategyNumber => {
            let cmpexact = if skey.sk_strategy == BTGreaterEqualStrategyNumber { 1 } else { 0 };
            if cmpresult >= cmpexact {
                matchelem += 1;
            }
            new_nelems = array.num_elems - matchelem;
            let m = matchelem as usize;
            array.elem_values.copy_within(m..m + new_nelems as usize, 0);
        }
        other => panic!("unrecognized StrategyNumber: {other}"),
    }

    debug_assert!(new_nelems >= 0 && new_nelems <= array.num_elems);
    array.num_elems = new_nelems;
    Ok(Some(new_nelems > 0))
}

/// _bt_skiparray_shrink: fold the scalar inequality at `skey` into the skip
/// array's low_compare/high_compare; Some(true) = key now redundant, None =
/// existing bound can't be compared against it.
fn bt_skiparray_shrink(
    rel: &Relation<'_>,
    so: &mut BTScanOpaqueData<'_>,
    skey: &ScanKeyData,
    array_i: i32,
) -> PgResult<Option<bool>> {
    debug_assert!(skey.sk_flags & (SK_ISNULL | SK_ROW_HEADER) == 0);
    let array = &mut so.arrayKeys[array_i as usize];
    debug_assert!(array.num_elems == -1);
    array.null_elem = false;

    match skey.sk_strategy {
        BTLessStrategyNumber | BTLessEqualStrategyNumber => {
            if let Some(hc) = array.high_compare.as_ref() {
                match compare_scankey_args_scalar(rel, hc, skey, hc)? {
                    None => return Ok(None),
                    Some(false) => return Ok(Some(true)), // keep existing bound
                    Some(true) => {}
                }
            }
            array.high_compare = Some(skey.clone());
        }
        BTGreaterEqualStrategyNumber | BTGreaterStrategyNumber => {
            if let Some(lc) = array.low_compare.as_ref() {
                match compare_scankey_args_scalar(rel, lc, skey, lc)? {
                    None => return Ok(None),
                    Some(false) => return Ok(Some(true)), // keep existing bound
                    Some(true) => {}
                }
            }
            array.low_compare = Some(skey.clone());
        }
        other => panic!("unrecognized StrategyNumber: {other}"),
    }
    Ok(Some(true))
}

/// _bt_skiparray_strat_adjust: skip support turns a final > low_compare into
/// >= (and < high_compare into <=) so MINVAL/MAXVAL descents can carry
/// lower-order keys.
fn bt_skiparray_strat_adjust(
    rel: &Relation<'_>,
    arraysk: &ScanKeyData,
    array: &mut BTArrayKeyInfo<'_>,
    qual_ok: &mut bool,
) -> PgResult<()> {
    debug_assert!(arraysk.sk_flags & SK_BT_SKIP != 0);
    debug_assert!(array.num_elems == -1 && !array.null_elem && array.sksup.is_some());

    if array
        .high_compare
        .as_ref()
        .is_some_and(|k| k.sk_strategy == BTLessStrategyNumber)
    {
        bt_skiparray_strat_step(rel, arraysk.sk_attno as usize, array, qual_ok, false)?;
    }
    if array
        .low_compare
        .as_ref()
        .is_some_and(|k| k.sk_strategy == BTGreaterStrategyNumber)
    {
        bt_skiparray_strat_step(rel, arraysk.sk_attno as usize, array, qual_ok, true)?;
    }
    Ok(())
}

// _bt_skiparray_strat_decrement (is_low=false) / _bt_skiparray_strat_increment
// (is_low=true); over/underflow marks the qual unsatisfiable.
fn bt_skiparray_strat_step(
    rel: &Relation<'_>,
    attno: usize,
    array: &mut BTArrayKeyInfo<'_>,
    qual_ok: &mut bool,
    is_low: bool,
) -> PgResult<()> {
    let opfamily = rel.rd_opfamily[attno - 1];
    let opcintype = rel.rd_opcintype[attno - 1];
    let sksup = array.sksup.expect("caller checked");
    let key = if is_low {
        array.low_compare.as_mut()
    } else {
        array.high_compare.as_mut()
    }
    .expect("caller checked");

    // The transformation is only safe when the operator type matches the
    // index attribute's input opclass type (cross-type could skip matches).
    if key.sk_subtype != opcintype && key.sk_subtype != 0 {
        return Ok(());
    }

    let mut flow = false;
    let new_sk_argument = if is_low {
        (sksup.increment)(key.sk_argument, &mut flow)
    } else {
        (sksup.decrement)(key.sk_argument, &mut flow)
    };
    if flow {
        *qual_ok = false;
        return Ok(());
    }

    let newstrat = if is_low { BTGreaterEqualStrategyNumber } else { BTLessEqualStrategyNumber };
    let mut lookupstrat = newstrat;
    if key.sk_flags & SK_BT_DESC != 0 {
        lookupstrat = BTCommuteStrategyNumber(lookupstrat);
    }
    let op = lsyscache::get_opfamily_member(opfamily, opcintype, opcintype, lookupstrat as i16)?;
    if op == 0 {
        return Ok(());
    }
    let cmp_proc = lsyscache::get_opcode(op)?;
    if cmp_proc != 0 {
        key.sk_func = fmgr_info(cmp_proc)?;
        key.sk_argument = new_sk_argument;
        key.sk_strategy = newstrat;
    }
    Ok(())
}

// _bt_binsrch_array_skey, preprocessing arm (no cur_elem_trig): first element
// >= the search datum. sk_flags supplies DESC/NULLS_FIRST bits only.
fn bt_binsrch_array_skey_raw(
    frame: &mut crate::fcframe::OrderProcFrame,
    orderproc: &mut FmgrInfo,
    collation: ::types_core::Oid,
    sk_flags: i32,
    searchdatum: Datum,
    searchnull: bool,
    array: &BTArrayKeyInfo<'_>,
    set_elem_result: &mut i32,
) -> PgResult<i32> {
    debug_assert!(!searchnull, "SAOP arrays never search NULL in preprocessing");
    let mut low_elem: i32 = 0;
    let mut mid_elem: i32 = -1;
    let mut high_elem: i32 = array.num_elems - 1;
    let mut result: i32 = 0;

    while high_elem > low_elem {
        mid_elem = low_elem + (high_elem - low_elem) / 2;
        let arrdatum = array.elem_values[mid_elem as usize];
        result = frame.cmp_proc(orderproc, collation, searchdatum, arrdatum)?;
        if sk_flags & SK_BT_DESC != 0 {
            result = INVERT_COMPARE_RESULT(result);
        }
        if result == 0 {
            low_elem = mid_elem;
            break;
        }
        if result > 0 {
            low_elem = mid_elem + 1;
        } else {
            high_elem = mid_elem;
        }
    }

    if low_elem != mid_elem {
        result = frame.cmp_proc(
            orderproc,
            collation,
            searchdatum,
            array.elem_values[low_elem as usize],
        )?;
        if sk_flags & SK_BT_DESC != 0 {
            result = INVERT_COMPARE_RESULT(result);
        }
    }
    *set_elem_result = result;
    Ok(low_elem)
}

/// _bt_unmark_keys, extended with the array bookkeeping (orderProcs reorder +
/// arrayKeys scan_key remap/sort).
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
    let have_arrays = so.numArrayKeys > 0;
    let mut kept_procs: PgVec<'_, FmgrInfo> = PgVec::new_in(mcx);
    let mut unmarked_procs: PgVec<'_, FmgrInfo> = PgVec::new_in(mcx);
    let mut key_map: PgVec<'_, i32> = PgVec::new_in(mcx);
    if have_arrays {
        key_map.resize(n, 0);
    }

    for (i, key) in so.keyData.iter().enumerate() {
        if !unmarkikey[i] {
            if have_arrays {
                key_map[i] = kept.len() as i32;
                kept_procs.push(so.orderProcs[i].clone());
            }
            kept.push(key.clone());
        } else {
            debug_assert!(key.sk_flags & SK_BT_SKIP == 0, "skip arrays are never unmarked");
            debug_assert!(
                key.sk_flags & SK_ISNULL == 0
                    || key.sk_flags & (SK_BT_REQFWD | SK_BT_REQBKWD) == 0
            );
            if have_arrays {
                key_map[i] = ((n - nunmark) + unmarked.len()) as i32;
                unmarked_procs.push(so.orderProcs[i].clone());
            }
            let mut key = key.clone();
            key.sk_flags &= !(SK_BT_REQFWD | SK_BT_REQBKWD);
            if key.sk_flags & SK_ROW_HEADER != 0 {
                // SAFETY: SK_ROW_HEADER contract (scankey.rs); clears the
                // requiredness flags on the shared subkey array, as C.
                unsafe {
                    let mut subkey = key.sk_argument.as_usize() as *mut ScanKeyData;
                    debug_assert!((*subkey).sk_strategy == key.sk_strategy);
                    loop {
                        debug_assert!((*subkey).sk_flags & SK_ROW_MEMBER != 0);
                        (*subkey).sk_flags &= !(SK_BT_REQFWD | SK_BT_REQBKWD);
                        if (*subkey).sk_flags & SK_ROW_END != 0 {
                            break;
                        }
                        subkey = subkey.add(1);
                    }
                }
            }
            unmarked.push(key);
        }
    }
    so.keyData.clear();
    so.keyData.extend(kept.into_iter());
    so.keyData.extend(unmarked.into_iter());

    if have_arrays {
        so.orderProcs.clear();
        so.orderProcs.extend(kept_procs.into_iter());
        so.orderProcs.extend(unmarked_procs.into_iter());
        for array in so.arrayKeys.iter_mut() {
            array.scan_key = key_map[array.scan_key as usize];
        }
        so.arrayKeys.sort_by_key(|a| a.scan_key);
    }
    Ok(())
}

// _bt_num_array_keys: SAOP array count plus the skip arrays to backfill
// (one per most-significant attribute lacking a = input key).
fn bt_num_array_keys(
    rel: &Relation<'_>,
    input_keys: &[ScanKeyData],
    skip_eq_ops: &mut [::types_core::Oid; INDEX_MAX_KEYS as usize],
) -> PgResult<(usize, usize)> {
    let n_saop = input_keys
        .iter()
        .filter(|k| k.sk_flags & SK_SEARCHARRAY != 0)
        .count();

    let mut attno_skip: i16 = 1;
    let mut attno_inkey: i16 = 1;
    let mut attno_has_equal = false;
    let mut attno_has_rowcompare = false;
    let mut num_skip = 0usize;
    let mut prev_num_skip = 0usize;
    for i in 0..=input_keys.len() {
        while attno_skip < attno_inkey {
            let opfamily = rel.rd_opfamily[attno_skip as usize - 1];
            let opcintype = rel.rd_opcintype[attno_skip as usize - 1];
            let eq_op = lsyscache::get_opfamily_member(
                opfamily,
                opcintype,
                opcintype,
                BTEqualStrategyNumber as i16,
            )?;
            skip_eq_ops[attno_skip as usize - 1] = eq_op;
            if eq_op == 0 {
                return Ok((n_saop + prev_num_skip, prev_num_skip));
            }
            num_skip += 1;
            attno_skip += 1;
        }
        prev_num_skip = num_skip;

        if i == input_keys.len() {
            break;
        }
        let inkey = &input_keys[i];
        if attno_has_rowcompare {
            break;
        }
        if attno_inkey < inkey.sk_attno {
            if attno_has_equal {
                skip_eq_ops[attno_skip as usize - 1] = 0;
            } else {
                let opfamily = rel.rd_opfamily[attno_skip as usize - 1];
                let opcintype = rel.rd_opcintype[attno_skip as usize - 1];
                let eq_op = lsyscache::get_opfamily_member(
                    opfamily,
                    opcintype,
                    opcintype,
                    BTEqualStrategyNumber as i16,
                )?;
                skip_eq_ops[attno_skip as usize - 1] = eq_op;
                if eq_op == 0 {
                    break;
                }
                num_skip += 1;
            }
            attno_skip += 1;
            attno_inkey = inkey.sk_attno;
            attno_has_equal = false;
        }
        if inkey.sk_strategy == BTEqualStrategyNumber || inkey.sk_flags & SK_SEARCHNULL != 0 {
            attno_has_equal = true;
        }
        if inkey.sk_flags & SK_ROW_HEADER != 0 {
            attno_has_rowcompare = true;
        }
    }

    Ok((n_saop + num_skip, num_skip))
}

/// _bt_preprocess_array_keys: deconstruct SK_SEARCHARRAY arrays, sort/dedup
/// elements, merge redundant arrays, and fill so->arrayKeys/orderProcs.
/// Returns the modified copy of the input keys, or None with no arrays.
fn bt_preprocess_array_keys<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'_>,
    so: &mut BTScanOpaqueData<'mcx>,
    input_keys: &[ScanKeyData],
) -> PgResult<Option<PgVec<'mcx, ScanKeyData>>> {
    let mut skip_eq_ops = [0 as ::types_core::Oid; INDEX_MAX_KEYS as usize];
    let (num_array_keys, num_skip_array_keys) =
        bt_num_array_keys(rel, input_keys, &mut skip_eq_ops)?;
    so.skipScan = num_skip_array_keys > 0;

    if num_array_keys == 0 {
        return Ok(None);
    }

    let indoption = &rel.rd_indoption;
    let mut array_key_data: PgVec<'mcx, ScanKeyData> = PgVec::new_in(mcx);
    array_key_data.reserve(input_keys.len() + num_skip_array_keys);
    so.arrayKeys.clear();
    so.arrayKeys.reserve(num_array_keys);
    so.orderProcs.clear();
    so.orderProcs.resize_with(input_keys.len() + num_skip_array_keys, FmgrInfo::unresolved);

    let mut origarrayatt: i16 = 0;
    let mut origarraykey: i32 = -1;
    let mut origelemtype: ::types_core::Oid = 0;
    let mut frame = crate::fcframe::OrderProcFrame::new();
    let mut num_skip_remaining = num_skip_array_keys;
    let mut attno_skip: i16 = 1;

    for inkey in input_keys.iter() {
        while num_skip_remaining > 0 && attno_skip <= inkey.sk_attno {
            let opfamily = rel.rd_opfamily[attno_skip as usize - 1];
            let opcintype = rel.rd_opcintype[attno_skip as usize - 1];
            let collation = rel.rd_indcollation[attno_skip as usize - 1];
            let eq_op = skip_eq_ops[attno_skip as usize - 1];

            if eq_op == 0 {
                // Attribute already has an = input key; copy it below instead.
                debug_assert!(attno_skip == inkey.sk_attno);
                attno_skip += 1;
                break;
            }

            let cmp_proc = lsyscache::get_opcode(eq_op)?;
            if cmp_proc == 0 {
                return Err(Box::new(PgError::error(format!(
                    "missing oprcode for skipping equals operator {eq_op}"
                ))));
            }

            let skipkey = ScanKeyData {
                sk_flags: SK_SEARCHARRAY | SK_BT_SKIP,
                sk_attno: attno_skip,
                sk_strategy: BTEqualStrategyNumber,
                sk_subtype: 0,
                sk_collation: collation,
                sk_func: fmgr_info(cmp_proc)?,
                sk_argument: Datum::null(),
            };

            let attr = rel.rd_att.compact_attr(attno_skip as usize - 1);
            let reverse = (indoption[attno_skip as usize - 1] as i32) & 0x0001 != 0;
            let (orderproc, _) = bt_setup_array_cmp(rel, &skipkey, opcintype, false)?;
            so.orderProcs[array_key_data.len()] = orderproc;
            so.arrayKeys.push(BTArrayKeyInfo {
                scan_key: array_key_data.len() as i32,
                num_elems: -1,
                elem_values: PgVec::new_in(mcx),
                cur_elem: -1,
                attlen: attr.attlen,
                attbyval: attr.attbyval,
                null_elem: true,
                sksup: ::skipsupport::prepare_skip_support_from_opclass(
                    opfamily, opcintype, reverse,
                )?,
                low_compare: None,
                high_compare: None,
            });
            array_key_data.push(skipkey);

            num_skip_remaining -= 1;
            attno_skip += 1;
        }

        let mut cur = inkey.clone();

        if cur.sk_flags & SK_SEARCHARRAY == 0 {
            array_key_data.push(cur);
            continue;
        }

        debug_assert!(
            cur.sk_flags & (SK_ROW_HEADER | SK_SEARCHNULL | SK_SEARCHNOTNULL) == 0
        );

        if cur.sk_flags & SK_ISNULL != 0 {
            so.qual_ok = false;
            break;
        }

        let p = cur.sk_argument.as_usize() as *const u8;
        // DatumGetArrayTypeP: borrow in place on an inline 4-byte header; a
        // short (1B) image expands into `mcx` so ARR_* offsets hold
        // (bound-param arrays keep their packed form through datumCopy).
        // SAFETY: non-null array datum addresses a live varlena.
        let img: &[u8] = unsafe {
            let b0 = *p;
            if b0 & 0x01 == 0x01 {
                assert!(b0 != 0x01, "_bt_preprocess_array_keys: external toast array image");
                let total = ((b0 >> 1) & 0x7F) as usize;
                let payload = core::slice::from_raw_parts(p.add(1), total - 1);
                let mut v: PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(mcx, total - 1 + 4)?;
                ::mcx::vec_append_bytes(&mut v, &::datum::varlena::set_varsize_4b(total - 1 + 4))?;
                ::mcx::vec_append_bytes(&mut v, payload)?;
                v.leak()
            } else {
                assert!(b0 & 0x03 == 0, "_bt_preprocess_array_keys: compressed array image");
                core::slice::from_raw_parts(p, arrfn::arr_size(core::slice::from_raw_parts(p, 4)))
            }
        };
        let arr_elemtype = arrfn::arr_elemtype(img);
        let (elmlen, elmbyval, elmalign) = lsyscache::get_typlenbyvalalign(arr_elemtype)?;
        let (raw_values, nulls) =
            ::arrayfuncs::deconstruct_array(mcx, img, elmlen as i32, elmbyval, elmalign as u8, true)?;

        let mut elem_values: PgVec<'mcx, Datum> = PgVec::new_in(mcx);
        elem_values.reserve(raw_values.len());
        for (v, isnull) in raw_values.iter().zip(nulls.iter()) {
            if !*isnull {
                elem_values.push(*v);
            }
        }

        if elem_values.is_empty() {
            so.qual_ok = false;
            break;
        }

        let elemtype = if cur.sk_subtype != 0 {
            cur.sk_subtype
        } else {
            rel.rd_opcintype[cur.sk_attno as usize - 1]
        };

        match cur.sk_strategy {
            BTLessStrategyNumber | BTLessEqualStrategyNumber => {
                cur.sk_argument = bt_find_extreme_element(
                    rel,
                    &cur,
                    elemtype,
                    BTGreaterStrategyNumber,
                    &elem_values,
                )?;
                array_key_data.push(cur);
                continue;
            }
            BTEqualStrategyNumber => {}
            BTGreaterEqualStrategyNumber | BTGreaterStrategyNumber => {
                cur.sk_argument = bt_find_extreme_element(
                    rel,
                    &cur,
                    elemtype,
                    BTLessStrategyNumber,
                    &elem_values,
                )?;
                array_key_data.push(cur);
                continue;
            }
            other => panic!("unrecognized StrategyNumber: {other}"),
        }

        let (orderproc, mut sortproc) = bt_setup_array_cmp(rel, &cur, elemtype, true)?;
        so.orderProcs[array_key_data.len()] = orderproc;
        let sortproc = sortproc.as_mut().expect("sortproc requested");

        let reverse = (indoption[cur.sk_attno as usize - 1] as i32) & 0x0001 != 0;
        bt_sort_array_elements(&mut frame, &cur, sortproc, reverse, &mut elem_values)?;

        if origarrayatt == cur.sk_attno {
            let merged = {
                let orig = &mut so.arrayKeys[origarraykey as usize];
                bt_merge_arrays(
                    &mut frame,
                    rel,
                    &cur,
                    sortproc,
                    reverse,
                    origelemtype,
                    elemtype,
                    orig,
                    &elem_values,
                )?
            };
            if merged {
                if so.arrayKeys[origarraykey as usize].num_elems == 0 {
                    so.qual_ok = false;
                    break;
                }
                continue; // throw away this scan key/array
            }
        } else {
            origarrayatt = cur.sk_attno;
            origarraykey = so.arrayKeys.len() as i32;
            origelemtype = elemtype;
        }

        let num_elems = elem_values.len() as i32;
        so.arrayKeys.push(BTArrayKeyInfo {
            scan_key: array_key_data.len() as i32,
            num_elems,
            elem_values,
            cur_elem: -1,
            attlen: 0,
            attbyval: false,
            null_elem: false,
            sksup: None,
            low_compare: None,
            high_compare: None,
        });
        array_key_data.push(cur);
    }

    debug_assert!(num_skip_remaining == 0 || !so.qual_ok);
    so.numArrayKeys = so.arrayKeys.len() as i32;
    Ok(Some(array_key_data))
}

/// _bt_preprocess_array_keys_final: remap array->scan_key references to
/// so->keyData offsets, consolidate orderProcs, and turn single-element
/// arrays into plain equality keys.
fn bt_preprocess_array_keys_final(
    rel: &Relation<'_>,
    so: &mut BTScanOpaqueData<'_>,
    key_data_map: &[i32],
) -> PgResult<()> {
    debug_assert!(so.qual_ok);

    if so.numArrayKeys == 0 {
        return Ok(());
    }

    let mut arrayidx = 0usize;
    for output_ikey in 0..so.numberOfKeys as usize {
        debug_assert!(so.keyData[output_ikey].sk_strategy != 0);
        if so.keyData[output_ikey].sk_strategy != BTEqualStrategyNumber {
            continue;
        }

        let input_ikey = key_data_map[output_ikey] as usize;

        if so.keyData[output_ikey].sk_flags & SK_SEARCHARRAY == 0 {
            let outkey = &so.keyData[output_ikey];
            if outkey.sk_flags & SK_SEARCHNULL != 0 {
                continue;
            }
            if outkey.sk_flags & SK_BT_REQFWD == 0 {
                continue;
            }
            let elemtype = if outkey.sk_subtype != 0 {
                outkey.sk_subtype
            } else {
                rel.rd_opcintype[outkey.sk_attno as usize - 1]
            };
            let (orderproc, _) = bt_setup_array_cmp(rel, outkey, elemtype, false)?;
            so.orderProcs[output_ikey] = orderproc;
            continue;
        }

        let reordered = so.orderProcs[input_ikey].clone();
        so.orderProcs[output_ikey] = reordered;

        while arrayidx < so.numArrayKeys as usize {
            debug_assert!(
                so.arrayKeys[arrayidx].num_elems > 0 || so.arrayKeys[arrayidx].num_elems == -1
            );
            if so.arrayKeys[arrayidx].scan_key == input_ikey as i32 {
                so.arrayKeys[arrayidx].scan_key = output_ikey as i32;

                if so.arrayKeys[arrayidx].num_elems == 1 {
                    let outkey = &mut so.keyData[output_ikey];
                    outkey.sk_flags &= !SK_SEARCHARRAY;
                    outkey.sk_argument = so.arrayKeys[arrayidx].elem_values[0];
                    so.numArrayKeys -= 1;
                    if so.numArrayKeys == 0 {
                        return Ok(());
                    }
                    so.arrayKeys.remove(arrayidx);
                } else {
                    if so.arrayKeys[arrayidx].num_elems == -1
                        && so.arrayKeys[arrayidx].sksup.is_some()
                        && !so.arrayKeys[arrayidx].null_elem
                    {
                        let BTScanOpaqueData { keyData, arrayKeys, qual_ok, .. } = so;
                        bt_skiparray_strat_adjust(
                            rel,
                            &keyData[output_ikey],
                            &mut arrayKeys[arrayidx],
                            qual_ok,
                        )?;
                    }
                    arrayidx += 1;
                }
                break;
            }
            arrayidx += 1;
        }
    }
    debug_assert!(so.arrayKeys.len() == so.numArrayKeys as usize);
    Ok(())
}

// _bt_find_extreme_element.
fn bt_find_extreme_element(
    rel: &Relation<'_>,
    skey: &ScanKeyData,
    elemtype: ::types_core::Oid,
    strat: StrategyNumber,
    elems: &[Datum],
) -> PgResult<Datum> {
    debug_assert!(skey.sk_strategy != BTEqualStrategyNumber);
    let opfamily = rel.rd_opfamily[skey.sk_attno as usize - 1];
    let cmp_op = lsyscache::get_opfamily_member(opfamily, elemtype, elemtype, strat as i16)?;
    if cmp_op == 0 {
        return Err(Box::new(PgError::error(format!(
            "missing operator {strat}({elemtype},{elemtype}) in opfamily {opfamily}"
        ))));
    }
    let cmp_proc = lsyscache::get_opcode(cmp_op)?;
    if cmp_proc == 0 {
        return Err(Box::new(PgError::error(format!(
            "missing oprcode for operator {cmp_op}"
        ))));
    }
    let mut flinfo = fmgr_info(cmp_proc)?;

    debug_assert!(!elems.is_empty());
    let mut result = elems[0];
    for &e in &elems[1..] {
        if function_call2_coll(&mut flinfo, skey.sk_collation, e, result)?.as_bool() {
            result = e;
        }
    }
    Ok(result)
}

// _bt_setup_array_cmp: (orderproc, Some(same-type sortproc) when requested).
fn bt_setup_array_cmp(
    rel: &Relation<'_>,
    skey: &ScanKeyData,
    elemtype: ::types_core::Oid,
    want_sortproc: bool,
) -> PgResult<(FmgrInfo, Option<FmgrInfo>)> {
    debug_assert!(skey.sk_strategy == BTEqualStrategyNumber);
    let opcintype = rel.rd_opcintype[skey.sk_attno as usize - 1];

    if elemtype == opcintype {
        let proc = crate::search::order_procinfo(rel, skey.sk_attno as usize)?;
        let sort = if want_sortproc { Some(proc.clone()) } else { None };
        return Ok((proc, sort));
    }

    let opfamily = rel.rd_opfamily[skey.sk_attno as usize - 1];
    let cmp_proc =
        lsyscache::get_opfamily_proc(opfamily, opcintype, elemtype, BTORDER_PROC as i16)?;
    if cmp_proc == 0 {
        return Err(missing_cross_type_proc(rel, opcintype, elemtype, skey.sk_attno));
    }
    let orderproc = fmgr_info(cmp_proc)?;

    if !want_sortproc {
        return Ok((orderproc, None));
    }

    let cmp_proc =
        lsyscache::get_opfamily_proc(opfamily, elemtype, elemtype, BTORDER_PROC as i16)?;
    if cmp_proc == 0 {
        return Err(missing_cross_type_proc(rel, elemtype, elemtype, skey.sk_attno));
    }
    Ok((orderproc, Some(fmgr_info(cmp_proc)?)))
}

#[track_caller]
#[cold]
#[inline(never)]
fn missing_cross_type_proc(
    rel: &Relation<'_>,
    lefttype: ::types_core::Oid,
    righttype: ::types_core::Oid,
    attno: i16,
) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "missing support function {BTORDER_PROC}({lefttype},{righttype}) for attribute {attno} of index \"{}\"",
        rel.name()
    )))
}

// _bt_sort_array_elements: sort + dedup in place, index column order.
fn bt_sort_array_elements(
    frame: &mut crate::fcframe::OrderProcFrame,
    skey: &ScanKeyData,
    sortproc: &mut FmgrInfo,
    reverse: bool,
    elems: &mut PgVec<'_, Datum>,
) -> PgResult<()> {
    if elems.len() <= 1 {
        return Ok(());
    }

    let mut err: Option<Box<PgError>> = None;
    let collation = skey.sk_collation;
    {
        let cmp = |a: Datum, b: Datum, frame: &mut crate::fcframe::OrderProcFrame,
                   sortproc: &mut FmgrInfo, err: &mut Option<Box<PgError>>| -> i32 {
            if err.is_some() {
                return 0;
            }
            match frame.cmp_proc(sortproc, collation, a, b) {
                Ok(mut r) => {
                    if reverse {
                        r = INVERT_COMPARE_RESULT(r);
                    }
                    r
                }
                Err(e) => {
                    *err = Some(e);
                    0
                }
            }
        };
        elems.sort_by(|&a, &b| cmp(a, b, frame, sortproc, &mut err).cmp(&0));
        if err.is_none() {
            elems.dedup_by(|a, b| cmp(*a, *b, frame, sortproc, &mut err) == 0);
        }
    }
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

// _bt_merge_arrays: intersect `next` into `orig` in place; false when the
// opfamily lacks the cross-type ORDER proc.
#[allow(clippy::too_many_arguments)]
fn bt_merge_arrays(
    frame: &mut crate::fcframe::OrderProcFrame,
    rel: &Relation<'_>,
    skey: &ScanKeyData,
    sortproc: &mut FmgrInfo,
    reverse: bool,
    origelemtype: ::types_core::Oid,
    nextelemtype: ::types_core::Oid,
    orig: &mut BTArrayKeyInfo<'_>,
    elems_next: &[Datum],
) -> PgResult<bool> {
    debug_assert!(skey.sk_strategy == BTEqualStrategyNumber);

    let mut crosstype_proc: Option<FmgrInfo> = None;
    if origelemtype != nextelemtype {
        let cmp_proc = lsyscache::get_opfamily_proc(
            rel.rd_opfamily[skey.sk_attno as usize - 1],
            origelemtype,
            nextelemtype,
            BTORDER_PROC as i16,
        )?;
        if cmp_proc == 0 {
            return Ok(false);
        }
        crosstype_proc = Some(fmgr_info(cmp_proc)?);
    }
    let mergeproc = match crosstype_proc.as_mut() {
        Some(p) => p,
        None => sortproc,
    };

    let nelems_orig_start = orig.num_elems as usize;
    let mut merged = 0usize;
    let (mut i, mut j) = (0usize, 0usize);
    while i < nelems_orig_start && j < elems_next.len() {
        let oelem = orig.elem_values[i];
        let nelem = elems_next[j];
        let mut res = frame.cmp_proc(mergeproc, skey.sk_collation, oelem, nelem)?;
        if reverse {
            res = INVERT_COMPARE_RESULT(res);
        }
        if res == 0 {
            orig.elem_values[merged] = oelem;
            merged += 1;
            i += 1;
            j += 1;
        } else if res < 0 {
            i += 1;
        } else {
            j += 1;
        }
    }
    orig.num_elems = merged as i32;
    Ok(true)
}
