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
        // The spilling engine sources its sort/slab context from the
        // `top_memory_context` seam; every test that calls `begin_state` needs
        // it installed. Install it here (the lowest common setup) so the tests
        // do not depend on `cargo test` execution order.
        install_top_mcx_once();
        sortsupport_seams::apply_sort_comparator::set(
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
    let sortopt = nodes::TUPLESORT_ALLOWBOUNDED;
    let out = sort_datums(&[5, 1, 4, 2, 8, 3, 7, 6, 0, 9], sortopt, Some(3), false);
    assert_eq!(out, vec![0, 1, 2]);
    // The bounded path was used.
}

#[test]
fn bounded_reports_top_n_method() {
    let sortopt = nodes::TUPLESORT_ALLOWBOUNDED;
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

// ---------------------------------------------------------------------------
// In-memory BufFile provider for the external-merge tape tests. A leaf-crate
// test binary cannot install the workspace's real buffile/fd seams, so we back
// the 5 buffile ops logtape uses with a thread-local flat byte buffer keyed by
// an id stashed in `BufFile.curFile`. This faithfully exercises the F2 tape
// engine end-to-end (write runs out, rewind, merge, read back) without a real
// temp file.
// ---------------------------------------------------------------------------

use std::cell::RefCell;
use std::collections::HashMap;

thread_local! {
    /// id -> (bytes, cursor). The cursor is the next read/write byte offset.
    static FAKE_FILES: RefCell<HashMap<i32, (Vec<u8>, usize)>> = RefCell::new(HashMap::new());
    static NEXT_FAKE_ID: RefCell<i32> = const { RefCell::new(1) };
}

const FAKE_BLCKSZ: usize = types_core::BLCKSZ as usize;

/// Install the `top_memory_context` seam once for the whole test binary. The
/// spilling engine sources its slab/sort context from `TopMemoryContext`; the
/// tests stand in a leaked (process-lifetime) `MemoryContext`.
fn install_top_mcx_once() {
    static ONCE: Once = Once::new();
    fn make() -> Mcx<'static> {
        thread_local! {
            static TOP: Mcx<'static> = {
                let leaked: &'static mcx::MemoryContext =
                    Box::leak(Box::new(mcx::MemoryContext::new("test-top")));
                leaked.mcx()
            };
        }
        TOP.with(|m| *m)
    }
    ONCE.call_once(|| {
        mcxt_seams::top_memory_context::set(make);
    });
}

fn install_fake_buffile() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        use buffile_seams as bf;
        use nodes::nodehashjoin::BufFile;
        use types_storage::file::PGAlignedBlock;

        bf::buf_file_create_temp::set(|mcx: Mcx<'_>, _inter_xact: bool| {
            let id = NEXT_FAKE_ID.with(|n| {
                let mut n = n.borrow_mut();
                let id = *n;
                *n += 1;
                id
            });
            FAKE_FILES.with(|f| {
                f.borrow_mut().insert(id, (Vec::new(), 0));
            });
            Ok(mcx::alloc_in(
                mcx,
                BufFile {
                    numFiles: 0,
                    files: Vec::new(),
                    isInterXact: false,
                    dirty: false,
                    readOnly: false,
                    fileset: None,
                    name: None,
                    resowner: None,
                    // We stash the fake id here (the only spare int field).
                    curFile: id,
                    curOffset: 0,
                    pos: 0,
                    nbytes: 0,
                    buffer: PGAlignedBlock::default(),
                },
            )
            .unwrap())
        });

        bf::buf_file_close::set(|file: mcx::PgBox<'_, BufFile>| {
            FAKE_FILES.with(|f| {
                f.borrow_mut().remove(&file.curFile);
            });
            Ok(())
        });

        bf::buf_file_seek_block::set(|file: &mut BufFile, blknum: i64| {
            FAKE_FILES.with(|f| {
                let mut f = f.borrow_mut();
                let (_, cursor) = f.get_mut(&file.curFile).expect("fake file");
                *cursor = blknum as usize * FAKE_BLCKSZ;
            });
            Ok(0)
        });

        bf::buf_file_write::set(|file: &mut BufFile, data: &[u8]| {
            FAKE_FILES.with(|f| {
                let mut f = f.borrow_mut();
                let (bytes, cursor) = f.get_mut(&file.curFile).expect("fake file");
                if *cursor + data.len() > bytes.len() {
                    bytes.resize(*cursor + data.len(), 0);
                }
                bytes[*cursor..*cursor + data.len()].copy_from_slice(data);
                *cursor += data.len();
            });
            Ok(())
        });

        bf::buf_file_read_exact::set(|file: &mut BufFile, buf: &mut [u8]| {
            FAKE_FILES.with(|f| {
                let mut f = f.borrow_mut();
                let (bytes, cursor) = f.get_mut(&file.curFile).expect("fake file");
                let end = *cursor + buf.len();
                if end > bytes.len() {
                    return Err(PgError::error("fake buffile: read past EOF"));
                }
                buf.copy_from_slice(&bytes[*cursor..end]);
                *cursor = end;
                Ok(())
            })
        });

        // logtape uses seek_block / write / read_exact / create_temp / close.
        // buf_file_seek (the byte-granular variant) is not on logtape's path;
        // install a panic-free stub so an accidental call is loud, not silent.
        bf::buf_file_seek::set(|_file: &mut BufFile, _fileno: i32, _offset: i64, _whence: i32| {
            panic!("fake buffile: byte-granular BufFileSeek not used by logtape")
        });
    });
}

/// Build a Datum-sort engine with an explicit `work_mem` (kB), feed `vals`,
/// performsort, and collect the forward output — used to force a tape spill.
fn sort_datums_workmem(vals: &[i64], work_mem: i32) -> Vec<i64> {
    install_test_comparator();
    install_fake_buffile();
    let mut owned = begin_state(work_mem, TUPLESORT_NONE, SortVariantKind::Datum).unwrap();
    owned.with_mut(|state| {
        let mcx = state.mcx();
        state.base.nKeys = 1;
        state.base.haveDatum1 = true;
        state.base.tuples = false; // pass-by-value int4
        state.base.arg = SortVariantArg::Datum {
            datumType: 23,
            datumTypeLen: 4,
        };
        state.base.sortKeys.push(datum_sortkey(mcx, false));
        state.base.onlyKey = Some(0);
    });
    for &v in vals {
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
    let mut out = Vec::new();
    loop {
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
fn external_sort_datum_spills_and_sorts() {
    // workMem is forced to a 64KB floor; a SortTuple is wider than the C 24-byte
    // struct, so the INITIAL_MEMTUPSIZE (1024) reservation nearly fills 64KB and
    // any further growth LACKMEMs — the sort spills to tape (inittapes /
    // dumptuples / mergeruns / the SORTEDONTAPE or FINALMERGE gettuple path).
    let n = 5000i64;
    // A scrambled-but-deterministic permutation of 0..n.
    let vals: Vec<i64> = (0..n).map(|i| (i * 2654435761) % n).collect();
    let out = sort_datums_workmem(&vals, 64);
    let expected: Vec<i64> = (0..n).collect();
    assert_eq!(out, expected);
}

#[test]
fn external_sort_reports_external_method() {
    install_test_comparator();
    install_fake_buffile();
    let mut owned = begin_state(64, TUPLESORT_NONE, SortVariantKind::Datum).unwrap();
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
    });
    for v in (0..5000i64).map(|i| (i * 1103515245 + 12345) % 5000) {
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
    // External sort: the space type is Disk; the method is one of the external
    // variants (external sort if materialized, external merge if on-the-fly).
    assert_eq!(stats.spaceType, TuplesortSpaceType::SORT_SPACE_TYPE_DISK);
    assert!(
        stats.sortMethod == TuplesortMethod::SORT_TYPE_EXTERNAL_SORT
            || stats.sortMethod == TuplesortMethod::SORT_TYPE_EXTERNAL_MERGE,
        "unexpected method {:?}",
        stats.sortMethod
    );
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

// ===========================================================================
// F3a: the heap + datum begin entry points + put/get impls + multi-key tiebreak.
// ===========================================================================

/// Install the sortsupport seams the begin entry points need (idempotent): a
/// no-op `prepare_sort_support_from_ordering_op` that just installs the test
/// integer comparator token (mirrors the catalog lookup), plus `get_typlenbyval`.
fn install_begin_seams() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        install_test_comparator();
        sortsupport_seams::prepare_sort_support_from_ordering_op::set(
            |_ordering_op: Oid, ssup: &mut SortSupportData<'_>| {
                // Resolve the (test) comparator; no abbreviation, no reverse.
                ssup.comparator = Some(SortComparatorId(0));
                Ok(())
            },
        );
        lsyscache_seams::get_typlenbyval::set(
            // int4: len 4, by-value.
            |_typid: Oid| Ok((4i16, true)),
        );
    });
}

#[test]
fn begin_datum_end_to_end() {
    // Drive the real begin_datum_state + putdatum_impl + getdatum_impl through
    // the carrier (the seam path), sorting pass-by-value int4s.
    install_begin_seams();
    let ctx = mcx::MemoryContext::new("test-root");
    let mcx = ctx.mcx();

    let owned = tuplesort_begin_datum_state(
        23,    // int4
        97,    // ordering op (ignored by the stub)
        0,     // collation
        false, // nulls last
        4096,
        TUPLESORT_NONE,
    )
    .unwrap();
    let mut carrier = into_carrier(mcx, owned).unwrap();

    // begin_datum: by-value type => base.tuples == false, onlyKey set.
    with_sort_mut(&mut carrier, |s| {
        assert_eq!(s.base.nKeys, 1);
        assert!(!s.base.tuples);
        assert_eq!(s.base.onlyKey, Some(0));
    });

    for v in [5i64, 1, 4, 2, 3] {
        with_sort_mut(&mut carrier, |s| {
            tuplesort_putdatum_impl(s, Datum::ByVal(v as usize), false).unwrap();
        });
    }
    with_sort_mut(&mut carrier, |s| tuplesort_performsort(s).unwrap());

    let mut out = Vec::new();
    loop {
        let got: Option<i64> = with_sort_mut(&mut carrier, |s| {
            let (found, val, isnull) = tuplesort_getdatum_impl(s, true, false).unwrap();
            if found && !isnull {
                Some(val.as_usize() as i64)
            } else {
                None
            }
        });
        match got {
            None => break,
            Some(v) => out.push(v),
        }
    }
    assert_eq!(out, vec![1, 2, 3, 4, 5]);
}

/// A 2-column int4 `TupleDescData` for the heap tiebreak test.
fn two_int4_tupdesc<'mcx>(mcx: Mcx<'mcx>) -> TupleDescData<'mcx> {
    let compact = CompactAttribute {
        attcacheoff: -1,
        attlen: 4,
        attbyval: true,
        attispackable: false,
        atthasmissing: false,
        attisdropped: false,
        attgenerated: false,
        attnullability: 0,
        attalignby: 4,
    };
    let mut attr = FormData_pg_attribute::default();
    attr.atttypid = 23;
    attr.attlen = 4;
    attr.attbyval = true;
    attr.attalign = b'i' as i8;
    attr.attstorage = b'p' as i8;
    TupleDescData {
        natts: 2,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: mcx::slice_in(mcx, &[compact, compact]).unwrap(),
        attrs: mcx::slice_in(mcx, &[attr, attr]).unwrap(),
    }
}

#[test]
fn heap_multikey_tiebreak() {
    // A 2-column heap sort: order by col1 then col2. The leading-key compares
    // (comparetup_heap) tie on col1, so comparetup_heap_tiebreak deforms both
    // MinimalTuples and breaks the tie on col2.
    install_begin_seams();

    let mut owned = begin_state(4096, TUPLESORT_NONE, SortVariantKind::Heap).unwrap();
    owned.with_mut(|state| {
        let mcx = state.mcx();
        state.base.nKeys = 2;
        state.base.haveDatum1 = true;
        state.base.tuples = true;
        state.base.arg = SortVariantArg::Heap {
            tupDesc: two_int4_tupdesc(mcx),
        };
        // Two sort keys: col1 (attno 1), col2 (attno 2), both ascending.
        let mut k0 = SortSupportData::new(mcx);
        k0.comparator = Some(SortComparatorId(0));
        k0.ssup_attno = 1;
        let mut k1 = SortSupportData::new(mcx);
        k1.comparator = Some(SortComparatorId(0));
        k1.ssup_attno = 2;
        state.base.sortKeys.push(k0);
        state.base.sortKeys.push(k1);
        // nKeys == 2 => onlyKey NOT set (tiebreak required).
        state.base.onlyKey = None;
    });

    // Rows: (col1, col2). Sorting by (col1, col2) ascending.
    let rows = [(2i64, 9i64), (1, 5), (2, 1), (1, 8), (2, 4)];
    for (c1, c2) in rows {
        owned.with_mut(|state| {
            let mcx = state.mcx();
            let tupdesc = match &state.base.arg {
                SortVariantArg::Heap { tupDesc } => tupDesc.clone_in(mcx).unwrap(),
                _ => unreachable!(),
            };
            let values = [Datum::ByVal(c1 as usize), Datum::ByVal(c2 as usize)];
            let isnull = [false, false];
            let mtup = heaptuple::heap_form_minimal_tuple(
                mcx, &tupdesc, &values, &isnull, 0,
            )
            .unwrap();
            let stup = SortTuple {
                tuple: Some(TupleBody::Minimal(mtup)),
                datum1: Datum::ByVal(c1 as usize),
                isnull1: false,
                srctape: 0,
            };
            tuplesort_puttuple_common(state, stup, false, 0).unwrap();
        });
    }

    owned.with_mut(|state| tuplesort_performsort(state).unwrap());

    let mut out: Vec<(i64, i64)> = Vec::new();
    loop {
        let got: Option<(i64, i64)> = owned.with_mut(|state| {
            let mcx = state.mcx();
            let st = tuplesort_gettuple_common(state, true).unwrap()?;
            let tupdesc = match &state.base.arg {
                SortVariantArg::Heap { tupDesc } => tupDesc,
                _ => unreachable!(),
            };
            let cols = heap_deform_sort_minimal(mcx, &st, tupdesc).unwrap();
            Some((cols[0].0.as_usize() as i64, cols[1].0.as_usize() as i64))
        });
        match got {
            None => break,
            Some(p) => out.push(p),
        }
    }
    assert_eq!(out, vec![(1, 5), (1, 8), (2, 1), (2, 4), (2, 9)]);
}

#[test]
fn heap_singlekey_tie_order_matches_pg_quicksort() {
    // A single-key heap sort (ORDER BY four) over the physical row order PG
    // produces for `SELECT ... FROM tenk1 WHERE unique1 < 10`. The sort is NOT
    // stable: PG's qsort partitions equal-key runs to a specific permutation
    // that the regression `.out` encodes. With input order (four payload):
    //   four = [0, 2, 1, 2, 1, 0, 1, 3, 3, 0]
    //   pl   = [4, 2, 1, 6, 9, 8, 5, 3, 7, 0]   (unique1, the payload)
    // PG 18.3's ST_SORT yields per group:
    //   four=0 -> 0,8,4 ; four=1 -> 5,9,1 ; four=2 -> 6,2 ; four=3 -> 3,7
    install_begin_seams();

    let mut owned = begin_state(4096, TUPLESORT_NONE, SortVariantKind::Heap).unwrap();
    owned.with_mut(|state| {
        let mcx = state.mcx();
        // Single sort key on col1 (four). nKeys == 1, onlyKey set (no tiebreak).
        state.base.nKeys = 1;
        state.base.haveDatum1 = true;
        state.base.tuples = true;
        state.base.arg = SortVariantArg::Heap {
            tupDesc: two_int4_tupdesc(mcx),
        };
        let mut k0 = SortSupportData::new(mcx);
        k0.comparator = Some(SortComparatorId(0));
        k0.ssup_attno = 1;
        state.base.sortKeys.push(k0);
        state.base.onlyKey = Some(0);
    });

    // Rows: (four, unique1). Physical input order from tenk1 (unique1 < 10).
    let rows = [
        (0i64, 4i64),
        (2, 2),
        (1, 1),
        (2, 6),
        (1, 9),
        (0, 8),
        (1, 5),
        (3, 3),
        (3, 7),
        (0, 0),
    ];
    for (four, u1) in rows {
        owned.with_mut(|state| {
            let mcx = state.mcx();
            let tupdesc = match &state.base.arg {
                SortVariantArg::Heap { tupDesc } => tupDesc.clone_in(mcx).unwrap(),
                _ => unreachable!(),
            };
            let values = [Datum::ByVal(four as usize), Datum::ByVal(u1 as usize)];
            let isnull = [false, false];
            let mtup = heaptuple::heap_form_minimal_tuple(
                mcx, &tupdesc, &values, &isnull, 0,
            )
            .unwrap();
            let stup = SortTuple {
                tuple: Some(TupleBody::Minimal(mtup)),
                datum1: Datum::ByVal(four as usize),
                isnull1: false,
                srctape: 0,
            };
            tuplesort_puttuple_common(state, stup, false, 0).unwrap();
        });
    }

    owned.with_mut(|state| tuplesort_performsort(state).unwrap());

    let mut out: Vec<(i64, i64)> = Vec::new();
    loop {
        let got: Option<(i64, i64)> = owned.with_mut(|state| {
            let mcx = state.mcx();
            let st = tuplesort_gettuple_common(state, true).unwrap()?;
            let tupdesc = match &state.base.arg {
                SortVariantArg::Heap { tupDesc } => tupDesc,
                _ => unreachable!(),
            };
            let cols = heap_deform_sort_minimal(mcx, &st, tupdesc).unwrap();
            Some((cols[0].0.as_usize() as i64, cols[1].0.as_usize() as i64))
        });
        match got {
            None => break,
            Some(p) => out.push(p),
        }
    }
    // (four, unique1) in PG 18.3 quicksort order.
    assert_eq!(
        out,
        vec![
            (0, 0),
            (0, 8),
            (0, 4),
            (1, 5),
            (1, 9),
            (1, 1),
            (2, 6),
            (2, 2),
            (3, 3),
            (3, 7),
        ]
    );
}

#[test]
fn external_merge_singlekey_tie_order_is_stable() {
    // Spilling external-merge stability regression (aggregates.sql agg_group_4 /
    // agg_hash_4 EXCEPT must be empty). A single-key heap sort over rows whose
    // leading key has duplicates and whose payload is the input-arrival index.
    // The input is pre-sorted by the leading key (mirrors `ORDER BY g/2` over
    // `generate_series` — each initial run is "presorted", so the per-run qsort
    // returns immediately preserving arrival order). With work_mem forced to the
    // 64KB floor the sort spills, exercising inittapes/dumptuples/mergeruns and
    // the on-the-fly FINALMERGE gettuple path. For equal leading keys the output
    // MUST preserve the per-run arrival order (payload ascending within a key),
    // exactly as the in-memory qsort does — the merge heap must drain the
    // oldest (lowest source-tape) tuple first on an exact tie.
    install_test_comparator();
    install_fake_buffile();
    install_begin_seams();

    let n: i64 = 6000;
    let mut owned = begin_state(64, TUPLESORT_NONE, SortVariantKind::Heap).unwrap();
    owned.with_mut(|state| {
        let mcx = state.mcx();
        state.base.nKeys = 1;
        state.base.haveDatum1 = true;
        state.base.tuples = true;
        state.base.arg = SortVariantArg::Heap {
            tupDesc: two_int4_tupdesc(mcx),
        };
        let mut k0 = SortSupportData::new(mcx);
        k0.comparator = Some(SortComparatorId(0));
        k0.ssup_attno = 1;
        state.base.sortKeys.push(k0);
        state.base.onlyKey = Some(0);
    });

    // Rows (key, payload) = (i/2, i) for i in 0..n, in ascending i (== arrival)
    // order. Each key value k appears twice: payloads 2k and 2k+1.
    for i in 0..n {
        let key = i / 2;
        owned.with_mut(|state| {
            let mcx = state.mcx();
            let tupdesc = match &state.base.arg {
                SortVariantArg::Heap { tupDesc } => tupDesc.clone_in(mcx).unwrap(),
                _ => unreachable!(),
            };
            let values = [Datum::ByVal(key as usize), Datum::ByVal(i as usize)];
            let isnull = [false, false];
            let mtup = heaptuple::heap_form_minimal_tuple(
                mcx, &tupdesc, &values, &isnull, 0,
            )
            .unwrap();
            let stup = SortTuple {
                tuple: Some(TupleBody::Minimal(mtup)),
                datum1: Datum::ByVal(key as usize),
                isnull1: false,
                srctape: 0,
            };
            tuplesort_puttuple_common(state, stup, false, 0).unwrap();
        });
    }

    owned.with_mut(|state| tuplesort_performsort(state).unwrap());

    // Confirm we actually spilled (otherwise the test is vacuous).
    let stats = owned.with_mut(|state| tuplesort_get_stats(state));
    assert_eq!(
        stats.spaceType,
        TuplesortSpaceType::SORT_SPACE_TYPE_DISK,
        "test must force an external (spilling) sort"
    );

    let mut out: Vec<(i64, i64)> = Vec::new();
    loop {
        let got: Option<(i64, i64)> = owned.with_mut(|state| {
            let mcx = state.mcx();
            let st = tuplesort_gettuple_common(state, true).unwrap()?;
            let tupdesc = match &state.base.arg {
                SortVariantArg::Heap { tupDesc } => tupDesc,
                _ => unreachable!(),
            };
            let cols = heap_deform_sort_minimal(mcx, &st, tupdesc).unwrap();
            Some((cols[0].0.as_usize() as i64, cols[1].0.as_usize() as i64))
        });
        match got {
            None => break,
            Some(p) => out.push(p),
        }
    }

    // The full output must be (key ascending, payload ascending) = exactly the
    // input order, since input was already globally sorted that way.
    let expected: Vec<(i64, i64)> = (0..n).map(|i| (i / 2, i)).collect();
    assert_eq!(out, expected);
}
