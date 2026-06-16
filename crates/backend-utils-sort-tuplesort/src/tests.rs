//! F1 in-memory engine tests: the qsort / heap / bounded state machine, driven
//! through `begin_state` + a Datum sort over a test integer comparator.

use super::*;
use mcx::Mcx;
use std::sync::Once;
use types_sortsupport::{SortComparatorId, SortSupportData};

const TUPLESORT_NONE: i32 = 0;

/// Install a test `apply_sort_comparator` that compares two `ByVal` words as
/// signed integers (the `ssup_datum_signed_cmp` analogue). Idempotent.
fn install_test_comparator() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        backend_utils_sort_sortsupport_seams::apply_sort_comparator::set(
            |a: Datum<'_>, b: Datum<'_>, _ssup: &SortSupportData<'_>| {
                let xx = a.as_usize() as i64;
                let yy = b.as_usize() as i64;
                Ok(if xx < yy {
                    -1
                } else if xx > yy {
                    1
                } else {
                    0
                })
            },
        );
    });
}

/// A single-key Datum [`SortSupportData`] (no abbreviation, no reverse) for the
/// test comparator.
fn datum_sortkey<'mcx>(mcx: Mcx<'mcx>, reverse: bool) -> SortSupportData<'mcx> {
    let mut ssup = SortSupportData::new(mcx);
    ssup.ssup_reverse = reverse;
    ssup.ssup_nulls_first = false;
    ssup.comparator = Some(SortComparatorId(0));
    ssup
}

/// Build a Datum-sort engine bundle, feed `vals` (all non-null pass-by-value),
/// performsort, and collect the forward output as a Vec<i64>.
///
/// For a bounded sort we fetch exactly `bound` tuples (the C contract: fetching
/// past the bound `elog(ERROR)`s — callers of a top-N sort stop at the bound).
fn sort_datums(vals: &[i64], sortopt: i32, bound: Option<i64>, reverse: bool) -> Vec<i64> {
    install_test_comparator();

    let mut owned = begin_state(4096, sortopt, SortVariantKind::Datum).unwrap();
    owned.with_mut(|state| {
        let mcx = state.mcx();
        // begin_datum: nKeys = 1, one sort key, onlyKey set (no abbreviation).
        state.base.nKeys = 1;
        state.base.haveDatum1 = true;
        state.base.tuples = false; // pass-by-value datum type
        state.base.arg = SortVariantArg::Datum {
            datumType: 23, // int4
            datumTypeLen: 4,
        };
        state.base.sortKeys.push(datum_sortkey(mcx, reverse));
        state.base.onlyKey = Some(0);
    });

    if let Some(b) = bound {
        owned.with_mut(|state| tuplesort_set_bound(state, b));
    }

    // putdatum (pass-by-value): stup.datum1 = val, isnull1 = false, tuple = None.
    for &v in vals {
        owned.with_mut(|state| {
            let stup = SortTuple {
                tuple: None,
                datum1: Datum::ByVal(v as usize),
                isnull1: false,
                srctape: 0,
            };
            // useAbbrev = false (no converter); tuplen = 0 for by-value.
            tuplesort_puttuple_common(state, stup, false, 0).unwrap();
        });
    }

    owned.with_mut(|state| tuplesort_performsort(state).unwrap());

    let mut out = Vec::new();
    // For a bounded sort, fetch exactly `bound` tuples (fetching past the bound
    // is the C `elog(ERROR)`). For an unbounded sort, fetch until EOF.
    let limit = bound.map(|b| b as usize);
    loop {
        if let Some(lim) = limit {
            if out.len() >= lim {
                break;
            }
        }
        // Extract the scalar inside the universal-`'mcx` closure: the SortTuple
        // borrows the bundle context, so only an owned non-`'mcx` value (the
        // i64) may leave.
        let got: Option<i64> = owned.with_mut(|state| {
            tuplesort_gettuple_common(state, true)
                .unwrap()
                .map(|st| st.datum1.as_usize() as i64)
        });
        match got {
            None => break,
            Some(v) => out.push(v),
        }
    }
    out
}

#[test]
fn inmem_quicksort_ascending() {
    let out = sort_datums(&[5, 1, 4, 2, 8, 3, 7, 6, 0, 9], TUPLESORT_NONE, None, false);
    assert_eq!(out, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
}

#[test]
fn inmem_quicksort_with_duplicates() {
    let out = sort_datums(&[3, 1, 3, 2, 1, 3, 2], TUPLESORT_NONE, None, false);
    assert_eq!(out, vec![1, 1, 2, 2, 3, 3, 3]);
}

#[test]
fn inmem_quicksort_reverse() {
    // ssup_reverse flips the comparator sign: descending output.
    let out = sort_datums(&[5, 1, 4, 2, 3], TUPLESORT_NONE, None, true);
    assert_eq!(out, vec![5, 4, 3, 2, 1]);
}

#[test]
fn inmem_single_element() {
    let out = sort_datums(&[42], TUPLESORT_NONE, None, false);
    assert_eq!(out, vec![42]);
}

#[test]
fn inmem_empty() {
    let out = sort_datums(&[], TUPLESORT_NONE, None, false);
    assert!(out.is_empty());
}

#[test]
fn bounded_top_n_heapsort() {
    // bound = 3 keeps the smallest 3. The bounded heap kicks in once the input
    // count exceeds 2*bound (or fills workMem with > bound tuples). With 10
    // inputs and bound 3 the engine switches to TSS_BOUNDED.
    let sortopt = types_nodes::TUPLESORT_ALLOWBOUNDED;
    let out = sort_datums(&[5, 1, 4, 2, 8, 3, 7, 6, 0, 9], sortopt, Some(3), false);
    assert_eq!(out, vec![0, 1, 2]);
    // The bounded path was used.
}

#[test]
fn bounded_reports_top_n_method() {
    let sortopt = types_nodes::TUPLESORT_ALLOWBOUNDED;
    install_test_comparator();
    let mut owned = begin_state(4096, sortopt, SortVariantKind::Datum).unwrap();
    owned.with_mut(|state| {
        let mcx = state.mcx();
        state.base.nKeys = 1;
        state.base.haveDatum1 = true;
        state.base.tuples = false;
        state.base.arg = SortVariantArg::Datum {
            datumType: 23,
            datumTypeLen: 4,
        };
        state.base.sortKeys.push(datum_sortkey(mcx, false));
        state.base.onlyKey = Some(0);
        tuplesort_set_bound(state, 2);
    });
    for v in [9_i64, 1, 5, 2, 7, 3] {
        owned.with_mut(|state| {
            let stup = SortTuple {
                tuple: None,
                datum1: Datum::ByVal(v as usize),
                isnull1: false,
                srctape: 0,
            };
            tuplesort_puttuple_common(state, stup, false, 0).unwrap();
        });
    }
    owned.with_mut(|state| tuplesort_performsort(state).unwrap());
    let stats = owned.with_mut(|state| tuplesort_get_stats(state));
    assert_eq!(stats.sortMethod, TuplesortMethod::SORT_TYPE_TOP_N_HEAPSORT);
    assert!(owned.with(|state| tuplesort_used_bound(state)));
}

#[test]
fn stats_reports_quicksort_in_memory() {
    install_test_comparator();
    let mut owned = begin_state(4096, TUPLESORT_NONE, SortVariantKind::Datum).unwrap();
    owned.with_mut(|state| {
        let mcx = state.mcx();
        state.base.nKeys = 1;
        state.base.haveDatum1 = true;
        state.base.tuples = false;
        state.base.sortKeys.push(datum_sortkey(mcx, false));
        state.base.onlyKey = Some(0);
        let stup = SortTuple {
            tuple: None,
            datum1: Datum::ByVal(7),
            isnull1: false,
            srctape: 0,
        };
        tuplesort_puttuple_common(state, stup, false, 0).unwrap();
        tuplesort_performsort(state).unwrap();
    });
    let stats = owned.with_mut(|state| tuplesort_get_stats(state));
    assert_eq!(stats.sortMethod, TuplesortMethod::SORT_TYPE_QUICKSORT);
    assert_eq!(stats.spaceType, TuplesortSpaceType::SORT_SPACE_TYPE_MEMORY);
}

#[test]
fn merge_order_is_clamped() {
    // Minimum memory => MINORDER (6).
    assert_eq!(tuplesort_merge_order(0), MINORDER);
    // Huge memory => clamped to MAXORDER (500).
    assert_eq!(tuplesort_merge_order(i64::MAX / 2), MAXORDER);
}

#[test]
fn method_and_space_names() {
    assert_eq!(
        tuplesort_method_name(TuplesortMethod::SORT_TYPE_QUICKSORT),
        "quicksort"
    );
    assert_eq!(
        tuplesort_method_name(TuplesortMethod::SORT_TYPE_TOP_N_HEAPSORT),
        "top-N heapsort"
    );
    assert_eq!(
        tuplesort_space_type_name(TuplesortSpaceType::SORT_SPACE_TYPE_DISK),
        "Disk"
    );
    assert_eq!(
        tuplesort_space_type_name(TuplesortSpaceType::SORT_SPACE_TYPE_MEMORY),
        "Memory"
    );
}

#[test]
fn carrier_round_trip() {
    // The type-erased carrier downcast path used by every seam.
    install_test_comparator();
    let ctx = mcx::MemoryContext::new("test");
    let mcx = ctx.mcx();
    let owned = begin_state(4096, TUPLESORT_NONE, SortVariantKind::Datum).unwrap();
    let mut carrier = into_carrier(mcx, owned).unwrap();
    with_sort_mut(&mut carrier, |state| {
        assert_eq!(state.variant, SortVariantKind::Datum);
        assert_eq!(state.status, TupSortStatus::Initial);
        assert_eq!(state.memtupsize, INITIAL_MEMTUPSIZE);
    });
}
