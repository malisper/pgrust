//! ginlogic.c: bool/tri-state consistent dispatch. A column carries proc 4
//! and/or proc 6 (initGinState errors when both are missing); the missing
//! one is shimmed over the other exactly as ginInitConsistentFunction
//! wires it (shimBoolConsistentFn / shimTriConsistentFn), and the EVERYTHING
//! search mode short-circuits to the true fns. `tmp` is C's so->tempCtx,
//! reset after every call.

use ::gin_vocab::*;
use ::mcx::MemoryContext;
use ::types_error::PgResult;

use crate::opclass;

/// directBoolConsistentFn over the column's proc 4 with `entry_res` in
/// place of key->entryRes: returns (result, recheckCurItem). C initializes
/// *recheck = true before the call.
fn direct_bool_consistent(
    tmp: &MemoryContext,
    state: &GinState,
    key: &GinScanKeyData<'_>,
    entry_res: &[GinTernaryValue],
    trgm_graph: Option<&mut TrgmPackedGraph>,
) -> PgResult<(bool, bool)> {
    let mut recheck = true;
    let res = opclass::consistent(
        tmp.mcx(),
        state.col(key.attnum),
        entry_res,
        key.strategy,
        key.query,
        key.nuserentries as usize,
        key.queryValues.as_slice(),
        key.queryCategories.as_slice(),
        key.jspOps.as_slice(),
        key.mapItemOperand.as_slice(),
        trgm_graph,
        &mut recheck,
    )?;
    Ok((res, recheck))
}

/// directTriConsistentFn over the column's proc 6.
fn direct_tri_consistent(
    tmp: &MemoryContext,
    state: &GinState,
    key: &mut GinScanKeyData<'_>,
) -> PgResult<GinTernaryValue> {
    opclass::tri_consistent(
        tmp.mcx(),
        state.col(key.attnum),
        key.entryRes.as_slice(),
        key.strategy,
        key.query,
        key.nuserentries as usize,
        key.queryValues.as_slice(),
        key.queryCategories.as_slice(),
        key.jspOps.as_slice(),
        key.mapItemOperand.as_slice(),
        key.trgmGraph.as_mut(),
    )
}

/// boolConsistentFn dispatch: trueConsistentFn for the EVERYTHING mode,
/// directBoolConsistentFn when the column has proc 4, else
/// shimBoolConsistentFn (ginlogic.c:106-130: GIN_MAYBE is true + recheck).
pub(crate) fn bool_consistent(
    tmp: &mut MemoryContext,
    state: &GinState,
    key: &mut GinScanKeyData<'_>,
) -> PgResult<bool> {
    if key.searchMode == GIN_SEARCH_MODE_EVERYTHING {
        key.recheckCurItem = false;
        return Ok(true);
    }
    let res = if state.col(key.attnum).consistent.is_some() {
        // Force recheck unless the consistent fn says otherwise (C
        // initializes *recheck = true before the call).
        key.recheckCurItem = true;
        let mut graph = key.trgmGraph.take();
        let out = direct_bool_consistent(tmp, state, key, key.entryRes.as_slice(), graph.as_mut());
        key.trgmGraph = graph;
        let (res, recheck) = out?;
        key.recheckCurItem = recheck;
        res
    } else {
        let result = direct_tri_consistent(tmp, state, key)?;
        if result == GIN_MAYBE {
            key.recheckCurItem = true;
            true
        } else {
            key.recheckCurItem = false;
            result == GIN_TRUE
        }
    };
    tmp.reset();
    Ok(res)
}

/// triConsistentFn dispatch: trueTriConsistentFn for the EVERYTHING mode,
/// directTriConsistentFn when the column has proc 6, else
/// shimTriConsistentFn over proc 4.
pub(crate) fn tri_consistent(
    tmp: &mut MemoryContext,
    state: &GinState,
    key: &mut GinScanKeyData<'_>,
) -> PgResult<GinTernaryValue> {
    if key.searchMode == GIN_SEARCH_MODE_EVERYTHING {
        return Ok(GIN_TRUE);
    }
    let res = if state.col(key.attnum).tri_consistent.is_some() {
        direct_tri_consistent(tmp, state, key)?
    } else {
        // C twiddles key->entryRes in place and restores it; the probes here
        // run over a scratch copy of all nentries (ginlogic.c:159), the
        // consistent fn reading its first nuserentries as always.
        let entry_res: Vec<GinTernaryValue> = key.entryRes.as_slice().to_vec();
        let graph = std::cell::RefCell::new(key.trgmGraph.take());
        let call = |local: &[GinTernaryValue]| {
            let mut graph = graph.borrow_mut();
            direct_bool_consistent(tmp, state, key, local, graph.as_mut())
        };
        let out = shim_tri_consistent(&entry_res, &call);
        key.trgmGraph = graph.into_inner();
        out?
    };
    tmp.reset();
    Ok(res)
}

const MAX_MAYBE_ENTRIES: usize = 4;

/// shimTriConsistentFn (ginlogic.c:139-215): probe the binary consistent
/// function with every TRUE/FALSE combination of the MAYBE entries; the
/// same answer everywhere is the answer, a disagreement is GIN_MAYBE, and
/// TRUE with a recheck from any probe is GIN_MAYBE. `check` is the key's
/// whole entryRes (nentries long, ginlogic.c:159).
pub(crate) fn shim_tri_consistent(
    check: &[GinTernaryValue],
    call: &dyn Fn(&[GinTernaryValue]) -> PgResult<(bool, bool)>,
) -> PgResult<GinTernaryValue> {
    let nkeys = check.len();
    let mut maybe_entries = [0usize; MAX_MAYBE_ENTRIES];
    let mut nmaybe = 0usize;
    for i in 0..nkeys {
        if check[i] == GIN_MAYBE {
            if nmaybe >= MAX_MAYBE_ENTRIES {
                return Ok(GIN_MAYBE);
            }
            maybe_entries[nmaybe] = i;
            nmaybe += 1;
        }
    }

    if nmaybe == 0 {
        let (res, rc) = call(check)?;
        return Ok(if res && rc { GIN_MAYBE } else if res { GIN_TRUE } else { GIN_FALSE });
    }

    let mut local: Vec<GinTernaryValue> = check[..nkeys].to_vec();
    for &e in &maybe_entries[..nmaybe] {
        local[e] = GIN_FALSE;
    }
    // ginlogic.c:184: recheck = key->recheckCurItem of the all-FALSE probe,
    // OR-ed with every later combination.
    let (first, first_rc) = call(&local)?;
    let cur_result = first;
    let mut recheck = first_rc;
    loop {
        let mut i = 0usize;
        while i < nmaybe {
            let e = maybe_entries[i];
            if local[e] == GIN_FALSE {
                local[e] = GIN_TRUE;
                break;
            }
            local[e] = GIN_FALSE;
            i += 1;
        }
        if i == nmaybe {
            break;
        }
        let (res, rc) = call(&local)?;
        recheck |= rc;
        if cur_result != res {
            return Ok(GIN_MAYBE);
        }
    }
    Ok(if cur_result && recheck {
        GIN_MAYBE
    } else if cur_result {
        GIN_TRUE
    } else {
        GIN_FALSE
    })
}

#[cfg(test)]
mod rem_b006_tests {
    use super::*;

    // ginlogic.c:184 shimTriConsistentFn: `recheck = key->recheckCurItem`
    // after the all-FALSE probe, then OR-ed with every other combination.
    // A candidate that matches only-with-recheck when the MAYBE entries are
    // absent, and unconditionally when present, is GIN_MAYBE in C (heap
    // recheck forced); dropping the first probe's recheck yields GIN_TRUE
    // and lets non-matching rows through
    // (row a186-candidate-fp-gin-b1-ebf7e9fc257bc3bc98ad-1).
    #[test]
    fn shim_tri_consistent_keeps_recheck_of_all_false_probe() {
        let check = [GIN_MAYBE, GIN_TRUE];
        let calls = std::cell::Cell::new(0u32);
        let consistent = |local: &[i8]| -> PgResult<(bool, bool)> {
            calls.set(calls.get() + 1);
            // Match either way; recheck only when the MAYBE entry is absent.
            Ok((true, local[0] == GIN_FALSE))
        };
        let res = shim_tri_consistent(&check, &consistent).unwrap();
        assert_eq!(calls.get(), 2, "both combinations of the one MAYBE entry are probed");
        assert_eq!(res, GIN_MAYBE, "TRUE with recheck from the all-FALSE probe is GIN_MAYBE");

        // Control: no combination needs a recheck -> GIN_TRUE.
        let plain = |_local: &[i8]| -> PgResult<(bool, bool)> { Ok((true, false)) };
        assert_eq!(shim_tri_consistent(&check, &plain).unwrap(), GIN_TRUE);
        // Control: the result flips across combinations -> GIN_MAYBE.
        let flip = |local: &[i8]| -> PgResult<(bool, bool)> { Ok((local[0] == GIN_TRUE, false)) };
        assert_eq!(shim_tri_consistent(&check, &flip).unwrap(), GIN_MAYBE);
    }
}
