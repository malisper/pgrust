// tuplesort.c + tuplesortvariants.c serial core, in-memory + external
// (spill-to-tape balanced k-way merge in tape.rs over sort_storage's
// logtape); parallel sort and cstring datum sorts = loud panics naming C.
#![allow(non_snake_case)]

use core::cell::Cell;
use core::mem;

use ::datum::{Datum, NullableDatum};
use ::mcx::{McxOwned, Mcx, MemoryContext, PgVec};
use ::types_core::instrument::{TuplesortInstrumentation, TuplesortMethod, TuplesortSpaceType};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult, ERRCODE_UNIQUE_VIOLATION};
use ::types_slot::SlotData;
use ::types_tuple::itemptr::ItemPointerData;
use ::types_tuple::{MinimalTupleData, TupleDescData};

mod abbrev;
mod mgetattr;
mod qsort;
mod radix;
mod ssup;

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod testhooks {
    thread_local! {
        pub static MKSORT_DISABLE: std::cell::Cell<bool> =
            const { std::cell::Cell::new(false) };
        pub static RADIX_DISABLE: std::cell::Cell<bool> =
            const { std::cell::Cell::new(false) };
        pub static RADIX_ATTEMPTS: std::cell::Cell<u32> =
            const { std::cell::Cell::new(0) };
        pub static RADIX_COMPLETED: std::cell::Cell<u32> =
            const { std::cell::Cell::new(0) };
    }
}

pub use abbrev::AbbrevState;
pub use ssup::{
    apply_cmp, apply_sort_comparator_in, comparator_for_index_col,
    comparator_for_opfamily,
    prepare_sort_support_abbrev, prepare_sort_support_from_ordering_op, AbbrevArm, AbbrevKind,
    SortComparator, SortSupport, SortSupportInit,
};

use mgetattr::minimal_getattr;
use qsort::qsort_tuple;

pub fn init_seams() {
    tuplesort_seams::tuplesort_datums::set(
        |mcx, datum_type, sort_operator, collation, nulls_first, work_mem, values| {
            let mut ts = Tuplesort::begin_datum(
                datum_type, sort_operator, collation, nulls_first, work_mem, TUPLESORT_NONE,
            )?;
            for v in values {
                ts.putdatum(v.value, v.isnull)?;
            }
            ts.performsort()?;
            let byref = ts.datum_sort_is_byref();
            let mut out: PgVec<'_, NullableDatum> = mcx::vec_with_capacity_in(mcx, values.len())?;
            while let Some(mut nd) = ts.getdatum(true)? {
                if byref && !nd.isnull {
                    let p = nd.value.as_usize() as *const u8;
                    // SAFETY: by-ref sorted datum points at a live plain image
                    // owned by the tuplesort, copied out before ts drops.
                    let bytes = unsafe {
                        core::slice::from_raw_parts(p, ::types_tuple::varatt::varsize_any(p))
                    };
                    nd.value = Datum::from_usize(mcx::slice_borrow_in(mcx, bytes)?.as_ptr() as usize);
                }
                out.push(nd);
            }
            Ok(out)
        },
    );
    tuplesort_seams::pgrcolumnar_ingest_sort::set(|tup_desc, keys, work_mem| {
        use tuplesort_seams::CbSortKeyKind;
        assert!(!keys.is_empty());
        let ssup: Vec<SortSupport> = keys
            .iter()
            .map(|&(attno, kind)| SortSupport {
                ssup_collation: ::types_core::catalog::C_COLLATION_OID,
                ssup_reverse: false,
                ssup_nulls_first: false,
                ssup_attno: attno,
                comparator: match kind {
                    CbSortKeyKind::Int16 => SortComparator::Int16,
                    CbSortKeyKind::Int32 => SortComparator::Int32,
                    CbSortKeyKind::Int64 => SortComparator::SignedI64,
                    CbSortKeyKind::TextC => SortComparator::TextC,
                },
            })
            .collect();
        let ts = Tuplesort::begin_heap_with_keys(tup_desc, &ssup, work_mem, TUPLESORT_NONE);
        Ok(Box::new(CbIngestSortImpl { ts }))
    });
}

// pgrcolumnar ingest-sort seam impl: a heap tuplesort driven value-wise.
struct CbIngestSortImpl {
    ts: Tuplesort,
}

impl tuplesort_seams::CbIngestSort for CbIngestSortImpl {
    fn put_row(&mut self, values: &[Datum], isnull: &[bool]) -> PgResult<()> {
        self.ts.putvalues(values, isnull)
    }

    fn sort(&mut self) -> PgResult<()> {
        self.ts.performsort()
    }

    fn next_row(&mut self, values: &mut [Datum], isnull: &mut [bool]) -> PgResult<bool> {
        self.ts.getvalues(true, values, isnull)
    }
}

pub const TUPLESORT_NONE: i32 = 0;
pub const TUPLESORT_RANDOMACCESS: i32 = 1 << 0;
pub const TUPLESORT_ALLOWBOUNDED: i32 = 1 << 1;

const INITIAL_MEMTUPSIZE: usize = 1024;
// Below this the tie-fallback snapshot + segment bookkeeping never pay.
const MKSORT_MIN: usize = 128;

#[inline(always)]
pub(crate) fn cfi() -> PgResult<()> {
    if init_small::globals::InterruptPending() {
        return cfi_slow();
    }
    Ok(())
}

#[cold]
#[inline(never)]
fn cfi_slow() -> PgResult<()> {
    postgres_seams::check_for_interrupts::call()
}

#[inline]
const fn maxalign(len: usize) -> usize {
    (len + 7) & !7
}

/// C SortTuple minus `srctape` (merge-only); same 24-byte cost shape.
#[derive(Clone, Copy)]
pub struct SortTuple {
    pub(crate) tuple: *mut MinimalTupleData,
    pub(crate) datum1: Datum,
    pub(crate) isnull1: bool,
}

// wasm32: 4-byte pointers pack SortTuple to 16; the 24-byte pin documents
// the native cost shape only.
#[cfg(not(target_family = "wasm"))]
const _: () = assert!(mem::size_of::<SortTuple>() == 24);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TupSortStatus {
    Initial,
    Bounded,
    BuildRuns,
    SortedInMem,
    SortedOnTape,
    FinalMerge,
}

enum SortVariant {
    Heap {
        tup_desc: std::rc::Rc<TupleDescData<'static>>,
    },
    // byref_typlen 0 = by-value; else datums copy into tuplecontext (C base->tuples).
    Datum { byref_typlen: i16 },
    Index {
        tup_desc: std::rc::Rc<TupleDescData<'static>>,
        nkeys: u16,
        enforce_unique: bool,
        unique_nulls_not_distinct: bool,
        index_name: std::rc::Rc<str>,
        // Lifetime-erased like tup_desc: the caller keeps the index open for
        // the life of the sort (unique-violation errdetail deparse); None in
        // relation-less unit tests.
        index_rel: Option<std::rc::Rc<types_rel::RelationData<'static>>>,
    },
    // tuplesort_begin_index_hash: (bucket, hash, TID) ordering off datum1.
    IndexHash {
        tup_desc: std::rc::Rc<TupleDescData<'static>>,
        high_mask: u32,
        low_mask: u32,
        max_buckets: u32,
    },
    // CLUSTER: full heap-tuple images sorted by btree index keys; attnums
    // are the heap attnos of the index key columns. index_desc is armed for
    // expression indexes (any key attnum == 0): the caller forms the index
    // key tuple per heap tuple (C instead evaluates FormIndexDatum inside
    // the comparator) and comparisons read it via index_getattr.
    Cluster {
        tup_desc: std::rc::Rc<TupleDescData<'static>>,
        attnums: [i16; 32],
        nkeys: u16,
        index_desc: Option<std::rc::Rc<TupleDescData<'static>>>,
    },
}

// Blob prefix for SortVariant::Cluster images (t_self survives the sort for
// the rewrite ctid-chain mapping); image starts MAXALIGNed at +16. With the
// expression-index lane armed, the formed index key tuple (itup_len bytes)
// follows at maxalign(16 + t_len).
#[repr(C)]
struct ClusterTupleHeader {
    t_len: u32,
    blk: u32,
    pos: u16,
    _pad: u16,
    itup_len: u32,
}
const _: () = assert!(mem::size_of::<ClusterTupleHeader>() == 16);


/// Trigger classes for `Tuplesort::topk_tie_ambiguity` (top-k boundary-tie
/// tracking, lane zone-adaptive sort feeds).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopkTieAmbiguity {
    /// Which rows made the LIMIT cut depends on arrival order.
    CutSelection,
    /// The cut set is exact; only equal-full-key emit order inside the
    /// output is arrival-dependent.
    RetainedOrder,
}

pub struct TuplesortData<'m> {
    mcx: Mcx<'m>,
    tuplecontext: MemoryContext,
    status: TupSortStatus,
    sortopt: i32,
    bounded: bool,
    bound_used: bool,
    bound: i32,
    avail_mem: i64,
    allowed_mem: i64,
    // Largest count below which a tuplen==0 put provably takes the pure
    // store-and-return path (no grow, no bounded transition, no lackmem);
    // 0 whenever status != Initial. u32 so the fast-path load cannot be
    // ldp-merged with the vec len (narrow-store→wide-load stall on V2).
    put_watermark: u32,
    grow_memtuples: bool,
    memtuples: PgVec<'m, SortTuple>,
    current: usize,
    eof_reached: bool,
    markpos_offset: usize,
    markpos_eof: bool,
    max_space: i64,
    max_space_status: TupSortStatus,
    sort_keys: PgVec<'m, SortSupport>,
    only_key: bool,
    have_datum1: bool,
    // Some = abbreviation armed: memtuple datum1 holds CONVERTED words (nulls
    // excepted); originals live only in `tuple`. None after abort/no-arm.
    // Boxed: ~2KB of HLL registers would push the hot tail fields off the
    // front cache lines (C pallocs ssup_extra separately too).
    abbrev: Option<Box<AbbrevState>>,
    abbrev_next: i64,
    // Freed-space size mode, resolved once (bounded-path discards are per-put).
    free_typlen: i16,
    // Top-k boundary-tie tracking (armed only by the lane's zone-adaptive
    // sort feed, docs/design/pgrcolumnar-zone-adaptive.md): `tie_dirty` records
    // whether the CURRENT k-th boundary key group extends beyond the heap —
    // i.e. an incoming full-key tie against the boundary was discarded, or a
    // boundary-tie heap member was evicted while an equal-key member remains.
    // Maintenance keeps it exact w.r.t. the FINAL boundary: a replace-top
    // that strictly improves the boundary clears it (all earlier tie events
    // were at keys the final boundary strictly beats). Off (false/false) the
    // put paths are untouched.
    tie_track: bool,
    tie_dirty: bool,
    // Top-k rowref total order (tie-ordering rule 2, lane zone-adaptive sort
    // feeds; armed via `arm_topk_rowref`, mutually exclusive with
    // `tie_track`): the bounded-heap and bounded-heapsort comparisons extend
    // full-key ties with the 48-bit physical rowref `puttupleslot_rowref`
    // stamped into the minimal tuple's `mt_padding`. Survivor selection at
    // the LIMIT cut becomes exact under the (key, rowref-ascending) total
    // order — precisely the physical-order feed's first-arrived survivors,
    // independent of arrival order — and retained ties emit in rowref
    // (physical) order. `rowref_missing` records a put that carried no
    // rowref while armed (contract violation; `topk_tie_ambiguity` then
    // reports `CutSelection` so the consumer demotes). Off (false/false)
    // every put/compare path is untouched.
    rowref_mode: bool,
    rowref_missing: bool,
    variant: SortVariant,
    // Unique violation recorded mid-sort, surfaced by performsort.
    unique_violation: Cell<Option<Box<PgError>>>,
    // Spill-only tail (declared last: the in-memory hot fields above keep
    // their pre-spill cache-line footprint). tuple_mem is C's tupleMem —
    // per-tuple memory alone so dumptuples can return exactly the tuple
    // share; tapes is Some from the first spill on (C tapeset != NULL).
    tuple_mem: i64,
    is_max_space_disk: bool,
    tapes: Option<Box<tape::TapeState<'m>>>,
}

::mcx::bind!(pub TuplesortTy => TuplesortData<'mcx>);

/// The C `Tuplesortstate *`; Drop is `tuplesort_end`. The Drop impl is the
/// fd guard: a spilled sort owns open temp-file VFDs that must close before
/// the query's resowner cross-check (C closes in tuplesort_end).
pub struct Tuplesort(McxOwned<TuplesortTy>);

impl Drop for Tuplesort {
    fn drop(&mut self) {
        self.0.with_mut(|st| {
            if let Some(ts) = st.tapes.take() {
                let _ = ts.tapeset.close();
            }
        })
    }
}

struct CmpCtx<'a> {
    mcx: ::mcx::Mcx<'a>,
    keys: &'a [SortSupport],
    only_key: bool,
    // Armed abbreviation (tiebreaks re-compare leading-key ORIGINALS with its
    // full_comparator). Borrow, not a resolved Option: ctx! runs per bounded
    // put and the resolve cost 7 instr/put there (micro tsort_bound100_100k).
    abbrev: &'a Option<Box<AbbrevState>>,
    variant: &'a SortVariant,
    unique_violation: &'a Cell<Option<Box<PgError>>>,
}

impl CmpCtx<'_> {
    #[inline]
    fn comparetup(&self, a: &SortTuple, b: &SortTuple) -> i32 {
        if let SortVariant::IndexHash { high_mask, low_mask, max_buckets, .. } = self.variant {
            return Self::comparetup_index_hash(*high_mask, *low_mask, *max_buckets, a, b);
        }
        let compare = ssup::apply_sort_comparator_in(
            self.mcx, a.datum1, a.isnull1, b.datum1, b.isnull1, &self.keys[0],
        );
        if compare != 0 {
            return compare;
        }
        self.comparetup_tiebreak(a, b)
    }

    /// comparetup_index_hash (+_tiebreak): bucket, then hash, then TID.
    fn comparetup_index_hash(
        high_mask: u32,
        low_mask: u32,
        max_buckets: u32,
        a: &SortTuple,
        b: &SortTuple,
    ) -> i32 {
        debug_assert!(!a.isnull1 && !b.isnull1);
        let hash1 = a.datum1.as_u32();
        let hash2 = b.datum1.as_u32();
        let bucket1 = types_hash::_hash_hashkey2bucket(hash1, max_buckets, high_mask, low_mask);
        let bucket2 = types_hash::_hash_hashkey2bucket(hash2, max_buckets, high_mask, low_mask);
        if bucket1 != bucket2 {
            return if bucket1 > bucket2 { 1 } else { -1 };
        }
        if hash1 != hash2 {
            return if hash1 > hash2 { 1 } else { -1 };
        }
        let tuple1: nbtree::itup::ITup = a.tuple.cast_const().cast();
        let tuple2: nbtree::itup::ITup = b.tuple.cast_const().cast();
        // SAFETY: t_tid header read of live images.
        let (tid1, tid2) = unsafe { (nbtree::itup::t_tid(tuple1), nbtree::itup::t_tid(tuple2)) };
        ::types_tuple::itemptr::ItemPointerCompare(&tid1, &tid2)
    }

    /// `qsort_tuple_{unsigned,signed,int32}_compare`: `cmp` folds per instantiation.
    #[inline(always)]
    fn comparetup_spec(&self, cmp: SortComparator, a: &SortTuple, b: &SortTuple) -> i32 {
        // SAFETY: every non-IndexHash variant carries >=1 sort key (begin_*
        // asserts); dispatch_cmp guards IndexHash. Per-compare bounds check
        // is C-unpaid work.
        let key0 = unsafe { self.keys.get_unchecked(0) };
        let compare = ssup::apply_sort_comparator_as_in(
            cmp, self.mcx, a.datum1, a.isnull1, b.datum1, b.isnull1, key0,
        );
        if compare != 0 {
            return compare;
        }
        if self.only_key {
            return 0;
        }
        self.comparetup_tiebreak(a, b)
    }

    /// comparetup_spec minus the datum1 null branches; caller proved the run
    /// isnull1-free, so results (and tie order) are identical.
    #[inline(always)]
    fn comparetup_spec_notnull(&self, cmp: SortComparator, a: &SortTuple, b: &SortTuple) -> i32 {
        debug_assert!(!a.isnull1 && !b.isnull1);
        // SAFETY: as comparetup_spec.
        let key0 = unsafe { self.keys.get_unchecked(0) };
        let c = ssup::apply_cmp_in(cmp, a.datum1, b.datum1, key0.ssup_collation, self.mcx);
        let compare = if key0.ssup_reverse { -c } else { c };
        if compare != 0 {
            return compare;
        }
        if self.only_key {
            return 0;
        }
        self.comparetup_tiebreak(a, b)
    }

    /// `comparetup_heap_tiebreak` / `comparetup_datum_tiebreak`: when abbrev
    /// is armed the leading key re-compares the ORIGINALS (datum1 words are
    /// abbreviations) via `ApplySortAbbrevFullComparator`; no-abbrev datum
    /// tiebreak reduces to 0.
    fn comparetup_tiebreak(&self, a: &SortTuple, b: &SortTuple) -> i32 {
        match self.variant {
            SortVariant::Heap { tup_desc } => {
                if let Some(abbrev) = self.abbrev {
                    let full = abbrev.full_comparator;
                    let key0 = &self.keys[0];
                    let attno = key0.ssup_attno as i32;
                    let (mut isnull1, mut isnull2) = (false, false);
                    // SAFETY: as the loop below — live minimal tuples under
                    // this descriptor.
                    let (datum1, datum2) = unsafe {
                        (
                            minimal_getattr(a.tuple, attno, tup_desc, &mut isnull1),
                            minimal_getattr(b.tuple, attno, tup_desc, &mut isnull2),
                        )
                    };
                    let compare = ssup::apply_sort_comparator_as_in(
                        full, self.mcx, datum1, isnull1, datum2, isnull2, key0,
                    );
                    if compare != 0 {
                        return compare;
                    }
                }
                for key in &self.keys[1..] {
                    let attno = key.ssup_attno as i32;
                    let (mut isnull1, mut isnull2) = (false, false);
                    // SAFETY: heap-variant SortTuples always carry a live minimal
                    // tuple copied under this descriptor.
                    let (datum1, datum2) = unsafe {
                        (
                            minimal_getattr(a.tuple, attno, tup_desc, &mut isnull1),
                            minimal_getattr(b.tuple, attno, tup_desc, &mut isnull2),
                        )
                    };
                    let compare =
                        ssup::apply_sort_comparator_in(self.mcx, datum1, isnull1, datum2, isnull2, key);
                    if compare != 0 {
                        return compare;
                    }
                }
                0
            }
            SortVariant::Datum { .. } => match self.abbrev {
                // datumCopy images parked in `tuple` are the originals.
                Some(abbrev) => ssup::apply_sort_comparator_as_in(
                    abbrev.full_comparator,
                    self.mcx,
                    Datum::from_usize(a.tuple as usize),
                    a.isnull1,
                    Datum::from_usize(b.tuple as usize),
                    b.isnull1,
                    &self.keys[0],
                ),
                None => 0,
            },
            SortVariant::Index { .. } => self.comparetup_index_btree_tiebreak(a, b),
            SortVariant::IndexHash { .. } => unreachable!("comparetup dispatches IndexHash whole"),
            // Index/cluster sorts never arm abbrev (index-build abbrev lane).
            SortVariant::Cluster { tup_desc, attnums, nkeys, index_desc } => {
                debug_assert!(self.abbrev.is_none());
                // comparetup_cluster_tiebreak, haveDatum1 lane (datum1 always
                // precomputed here, C's evaluate-in-comparator lane included);
                // no TID tiebreak (C leaves equal keys in qsort order).
                let (ta, tb) = unsafe {
                    (cluster_tuple_of(a.tuple), cluster_tuple_of(b.tuple))
                };
                for nkey in 1..*nkeys as usize {
                    let key = &self.keys[nkey];
                    let (mut isnull1, mut isnull2) = (false, false);
                    // SAFETY: blobs written by putheaptuple under these descriptors.
                    let (datum1, datum2) = unsafe {
                        match index_desc {
                            Some(idesc) => (
                                nbtree::itup::index_getattr(
                                    cluster_itup_of(a.tuple), (nkey + 1) as i16, idesc, &mut isnull1,
                                ),
                                nbtree::itup::index_getattr(
                                    cluster_itup_of(b.tuple), (nkey + 1) as i16, idesc, &mut isnull2,
                                ),
                            ),
                            None => (
                                ::types_tuple::heap_getattr(&ta, attnums[nkey] as i32, tup_desc, &mut isnull1),
                                ::types_tuple::heap_getattr(&tb, attnums[nkey] as i32, tup_desc, &mut isnull2),
                            ),
                        }
                    };
                    let compare = ssup::apply_sort_comparator_in(
                        self.mcx, datum1, isnull1, datum2, isnull2, key,
                    );
                    if compare != 0 {
                        return compare;
                    }
                }
                0
            }
        }
    }

    /// `comparetup_index_btree_tiebreak`, no abbrev arm. C divergence: the
    /// unique violation is deferred to performsort (no mid-qsort ereport).
    fn comparetup_index_btree_tiebreak(&self, a: &SortTuple, b: &SortTuple) -> i32 {
        debug_assert!(self.abbrev.is_none());
        let SortVariant::Index {
            tup_desc,
            nkeys,
            enforce_unique,
            unique_nulls_not_distinct,
            index_name,
            index_rel,
        } = self.variant
        else {
            unreachable!()
        };
        let tuple1: nbtree::itup::ITup = a.tuple.cast_const().cast();
        let tuple2: nbtree::itup::ITup = b.tuple.cast_const().cast();
        let mut equal_hasnull = a.isnull1;

        for nkey in 2..=(*nkeys as i16) {
            let key = &self.keys[nkey as usize - 1];
            let (mut isnull1, mut isnull2) = (false, false);
            // SAFETY: live tuplecontext images formed under this descriptor.
            let (datum1, datum2) = unsafe {
                (
                    nbtree::itup::index_getattr(tuple1, nkey, tup_desc, &mut isnull1),
                    nbtree::itup::index_getattr(tuple2, nkey, tup_desc, &mut isnull2),
                )
            };
            let compare =
                ssup::apply_sort_comparator_in(self.mcx, datum1, isnull1, datum2, isnull2, key);
            if compare != 0 {
                return compare;
            }
            if isnull1 {
                equal_hasnull = true;
            }
        }

        if *enforce_unique && !(!unique_nulls_not_distinct && equal_hasnull) {
            debug_assert!(!core::ptr::eq(tuple1, tuple2));
            let prev = self.unique_violation.take();
            self.unique_violation.set(Some(prev.unwrap_or_else(|| {
                unique_violation_error(index_name, index_rel.as_deref(), tup_desc, tuple1)
            })));
        }

        // SAFETY: t_tid header read of live images (contract above).
        let (tid1, tid2) = unsafe { (nbtree::itup::t_tid(tuple1), nbtree::itup::t_tid(tuple2)) };
        let compare = ::types_tuple::itemptr::ItemPointerCompare(&tid1, &tid2);
        debug_assert!(compare != 0, "ItemPointer values should never be equal");
        compare
    }
}

// comparetup_index_btree (tuplesortvariants.c): errdetail via the
// BuildIndexValueDescription seam; C's key_desc==NULL arm ("Duplicate keys
// exist.") covers the hidden-key gates and the uninstalled-seam test paths.
#[track_caller]
#[cold]
#[inline(never)]
fn unique_violation_error(
    index_name: &str,
    index_rel: Option<&types_rel::RelationData<'static>>,
    tup_desc: &TupleDescData<'static>,
    tuple: nbtree::itup::ITup,
) -> Box<PgError> {
    let key_desc = index_rel
        .filter(|_| genam_seams::build_index_value_description::is_installed())
        .and_then(|rel| {
            let natts = tup_desc.natts as usize;
            let mut values = [Datum::null(); types_core::INDEX_MAX_KEYS as usize];
            let mut isnull = [false; types_core::INDEX_MAX_KEYS as usize];
            for i in 0..natts {
                // SAFETY: live tuplecontext image formed under this descriptor.
                values[i] = unsafe {
                    nbtree::itup::index_getattr(tuple, (i + 1) as i16, tup_desc, &mut isnull[i])
                };
            }
            genam_seams::build_index_value_description::call(
                rel,
                &values[..natts],
                &isnull[..natts],
            )
            .unwrap_or(None)
        });
    let detail = match key_desc {
        Some(desc) => format!("Key {desc} is duplicated."),
        None => "Duplicate keys exist.".to_string(),
    };
    Box::new(
        PgError::error(format!("could not create unique index \"{index_name}\""))
            .with_sqlstate(ERRCODE_UNIQUE_VIOLATION)
            .with_detail(detail),
    )
}

macro_rules! ctx {
    ($st:expr) => {
        CmpCtx {
            mcx: $st.mcx,
            keys: &$st.sort_keys,
            only_key: $st.only_key,
            abbrev: &$st.abbrev,
            variant: &$st.variant,
            unique_violation: &$st.unique_violation,
        }
    };
}

/// C's ssup pattern: comparator identity resolved ONCE per sort operation,
/// compares monomorphized (no per-compare variant/shim ladder). M1-hot arms
/// first. One `$body` instantiation per arm = C's ST_DEFINE cost shape.
macro_rules! dispatch_cmp {
    ($ctx:expr, |$cmp:ident| $body:expr) => {
        dispatch_cmp!(@via comparetup_spec, $ctx, |$cmp| $body)
    };
    (@via $meth:ident, $ctx:expr, |$cmp:ident| $body:expr) => {{
        let __c = &$ctx;
        match __c.variant {
            SortVariant::IndexHash { high_mask, low_mask, max_buckets, .. } => {
                let (high_mask, low_mask, max_buckets) = (*high_mask, *low_mask, *max_buckets);
                let $cmp = |a: &SortTuple, b: &SortTuple| {
                    CmpCtx::comparetup_index_hash(high_mask, low_mask, max_buckets, a, b)
                };
                $body
            }
            // SAFETY: non-IndexHash variants carry >=1 key (begin_* asserts).
            _ => match unsafe { __c.keys.get_unchecked(0) }.comparator {
                SortComparator::Unsigned => {
                    let $cmp = |a: &SortTuple, b: &SortTuple| {
                        __c.$meth(SortComparator::Unsigned, a, b)
                    };
                    $body
                }
                SortComparator::SignedI64 => {
                    let $cmp = |a: &SortTuple, b: &SortTuple| {
                        __c.$meth(SortComparator::SignedI64, a, b)
                    };
                    $body
                }
                SortComparator::Int32 => {
                    let $cmp = |a: &SortTuple, b: &SortTuple| {
                        __c.$meth(SortComparator::Int32, a, b)
                    };
                    $body
                }
                SortComparator::Int16 => {
                    let $cmp = |a: &SortTuple, b: &SortTuple| {
                        __c.$meth(SortComparator::Int16, a, b)
                    };
                    $body
                }
                SortComparator::Uint32 => {
                    let $cmp = |a: &SortTuple, b: &SortTuple| {
                        __c.$meth(SortComparator::Uint32, a, b)
                    };
                    $body
                }
                SortComparator::Float32 => {
                    let $cmp = |a: &SortTuple, b: &SortTuple| {
                        __c.$meth(SortComparator::Float32, a, b)
                    };
                    $body
                }
                SortComparator::Float64 => {
                    let $cmp = |a: &SortTuple, b: &SortTuple| {
                        __c.$meth(SortComparator::Float64, a, b)
                    };
                    $body
                }
                SortComparator::TextC => {
                    let $cmp = |a: &SortTuple, b: &SortTuple| {
                        __c.$meth(SortComparator::TextC, a, b)
                    };
                    $body
                }
                SortComparator::Interval => {
                    let $cmp = |a: &SortTuple, b: &SortTuple| {
                        __c.$meth(SortComparator::Interval, a, b)
                    };
                    $body
                }
                SortComparator::NameC => {
                    let $cmp = |a: &SortTuple, b: &SortTuple| {
                        __c.$meth(SortComparator::NameC, a, b)
                    };
                    $body
                }
                SortComparator::BpcharC => {
                    let $cmp = |a: &SortTuple, b: &SortTuple| {
                        __c.$meth(SortComparator::BpcharC, a, b)
                    };
                    $body
                }
                // strcoll dominates locale compares; nothing to fold.
                SortComparator::TextLocale(_) | SortComparator::BpcharLocale(_) => {
                    let $cmp = |a: &SortTuple, b: &SortTuple| __c.comparetup(a, b);
                    $body
                }
                SortComparator::Uuid => {
                    let $cmp = |a: &SortTuple, b: &SortTuple| {
                        __c.$meth(SortComparator::Uuid, a, b)
                    };
                    $body
                }
                SortComparator::Network => {
                    let $cmp = |a: &SortTuple, b: &SortTuple| {
                        __c.$meth(SortComparator::Network, a, b)
                    };
                    $body
                }
                SortComparator::NumericAbbrev => {
                    let $cmp = |a: &SortTuple, b: &SortTuple| {
                        __c.$meth(SortComparator::NumericAbbrev, a, b)
                    };
                    $body
                }
                // cmp_numerics dominates full numeric compares; nothing to fold.
                SortComparator::Numeric => {
                    let $cmp = |a: &SortTuple, b: &SortTuple| __c.comparetup(a, b);
                    $body
                }
                // Z-order interleave dominates; nothing to fold (C leaves
                // gist_bbox_zorder_cmp on the generic qsort too).
                SortComparator::GistPointZorder => {
                    let $cmp = |a: &SortTuple, b: &SortTuple| __c.comparetup(a, b);
                    $body
                }
                // Shim'd comparisons are fmgr calls; nothing to fold.
                SortComparator::Shim(_) => {
                    let $cmp = |a: &SortTuple, b: &SortTuple| __c.comparetup(a, b);
                    $body
                }
                // Extension gist opclass comparators (btree_gist sorted
                // builds); indirect call per compare, nothing to fold.
                SortComparator::GistOpclass(_) => {
                    let $cmp = |a: &SortTuple, b: &SortTuple| __c.comparetup(a, b);
                    $body
                }
                // Bool keys are cold catalog sorts; not worth a monomorph arm.
                SortComparator::Bool => {
                    let $cmp = |a: &SortTuple, b: &SortTuple| __c.comparetup(a, b);
                    $body
                }
            },
        }
    }};
}

// Declared after dispatch_cmp! so the macro is in scope inside the module.
mod tape;

pub use tape::tuplesort_merge_order;

impl Tuplesort {
    /// `tuplesort_begin_heap`.
    #[allow(clippy::too_many_arguments)]
    pub fn begin_heap(
        tup_desc: std::rc::Rc<TupleDescData<'static>>,
        att_nums: &[i16],
        sort_operators: &[Oid],
        sort_collations: &[Oid],
        nulls_first_flags: &[bool],
        work_mem: i32,
        sortopt: i32,
    ) -> PgResult<Tuplesort> {
        let nkeys = att_nums.len();
        assert!(nkeys > 0 && sort_operators.len() == nkeys && sort_collations.len() == nkeys
            && nulls_first_flags.len() == nkeys);
        let mut keys = Vec::with_capacity(nkeys);
        let mut abbrev_arm = None;
        for i in 0..nkeys {
            debug_assert!(att_nums[i] != 0 && sort_operators[i] != 0);
            let init = SortSupportInit {
                ssup_collation: sort_collations[i],
                ssup_nulls_first: nulls_first_flags[i],
                ssup_attno: att_nums[i],
            };
            // C: sortKey->abbreviate = (i == 0 && base->haveDatum1).
            let (key, arm) =
                ssup::prepare_sort_support_abbrev(sort_operators[i], &init, i == 0)?;
            keys.push(key);
            if i == 0 {
                abbrev_arm = arm;
            }
        }
        // onlyKey cannot be used with abbreviation (ties need the tiebreak).
        let only_key = nkeys == 1 && abbrev_arm.is_none();
        Ok(Self::begin_common(
            work_mem,
            sortopt,
            &keys,
            only_key,
            abbrev_arm.map(|arm| Box::new(AbbrevState::new(arm))),
            SortVariant::Heap { tup_desc },
        ))
    }

    /// C divergence: begin over pre-resolved sort keys (test/bench surface;
    /// `begin_heap` is the catalog path).
    pub fn begin_heap_with_keys(
        tup_desc: std::rc::Rc<TupleDescData<'static>>,
        keys: &[SortSupport],
        work_mem: i32,
        sortopt: i32,
    ) -> Tuplesort {
        assert!(!keys.is_empty());
        let only_key = keys.len() == 1;
        Self::begin_common(work_mem, sortopt, keys, only_key, None, SortVariant::Heap { tup_desc })
    }

    /// `tuplesort_begin_index_btree`, serial arm; keys read straight off the
    /// index relation — the same values C pulls via `_bt_mkscankey`.
    pub fn begin_index_btree(
        _heap_rel: &types_rel::Relation<'_>,
        index_rel: &types_rel::Relation<'_>,
        enforce_unique: bool,
        unique_nulls_not_distinct: bool,
        work_mem: i32,
        sortopt: i32,
    ) -> PgResult<Tuplesort> {
        const INDOPTION_DESC: i16 = 1 << 0;
        const INDOPTION_NULLS_FIRST: i16 = 1 << 1;
        let nkeys = index_rel.indnkeyatts() as usize;
        assert!(nkeys > 0);
        let mut keys = Vec::with_capacity(nkeys);
        for i in 0..nkeys {
            let indoption = index_rel.rd_indoption[i];
            let collation = index_rel.rd_indcollation[i];
            let comparator = comparator_for_index_col(
                index_rel.rd_opfamily[i],
                index_rel.rd_opcintype[i],
                collation,
            )?;
            keys.push(SortSupport {
                ssup_collation: collation,
                ssup_reverse: indoption & INDOPTION_DESC != 0,
                ssup_nulls_first: indoption & INDOPTION_NULLS_FIRST != 0,
                ssup_attno: (i + 1) as i16,
                comparator,
            });
        }
        // SAFETY (both): lifetime erasure on the relcache tupdesc/entry; the
        // caller keeps the index relation open for the life of the sort (C's
        // implicit contract — nbtsort holds it open across the whole build).
        let tup_desc: std::rc::Rc<TupleDescData<'static>> =
            unsafe { mem::transmute(index_rel.rd_att.clone()) };
        let index_rel_erased: std::rc::Rc<types_rel::RelationData<'static>> =
            unsafe { mem::transmute(index_rel.data_rc().clone()) };
        Ok(Self::begin_index_with_keys(
            tup_desc,
            &keys,
            nkeys as u16,
            enforce_unique,
            unique_nulls_not_distinct,
            index_rel.name(),
            Some(index_rel_erased),
            work_mem,
            sortopt,
        ))
    }

    /// `tuplesort_begin_index_gist`, serial arm; comparators resolved from
    /// each column's GIST_SORTSUPPORT_PROC (never reverse / nulls-first,
    /// as C's PrepareSortSupportFromGistIndexRel).
    pub fn begin_index_gist(
        _heap_rel: &types_rel::Relation<'_>,
        index_rel: &types_rel::Relation<'_>,
        work_mem: i32,
        sortopt: i32,
    ) -> PgResult<Tuplesort> {
        let nkeys = index_rel.indnkeyatts() as usize;
        assert!(nkeys > 0);
        let mut keys = Vec::with_capacity(nkeys);
        for i in 0..nkeys {
            let comparator = ssup::comparator_for_gist_index_col(
                index_rel.rd_opfamily[i],
                index_rel.rd_opcintype[i],
            )?;
            keys.push(SortSupport {
                ssup_collation: index_rel.rd_indcollation[i],
                ssup_reverse: false,
                ssup_nulls_first: false,
                ssup_attno: (i + 1) as i16,
                comparator,
            });
        }
        // SAFETY (both): lifetime erasure on the relcache tupdesc/entry; the
        // caller keeps the index relation open for the life of the sort
        // (gistbuild holds it open across the whole build, as C does).
        let tup_desc: std::rc::Rc<TupleDescData<'static>> =
            unsafe { mem::transmute(index_rel.rd_att.clone()) };
        let index_rel_erased: std::rc::Rc<types_rel::RelationData<'static>> =
            unsafe { mem::transmute(index_rel.data_rc().clone()) };
        Ok(Self::begin_index_with_keys(
            tup_desc,
            &keys,
            nkeys as u16,
            false,
            false,
            index_rel.name(),
            Some(index_rel_erased),
            work_mem,
            sortopt,
        ))
    }

    /// `tuplesort_begin_index_hash`, serial arm.
    pub fn begin_index_hash(
        _heap_rel: &types_rel::Relation<'_>,
        index_rel: &types_rel::Relation<'_>,
        high_mask: u32,
        low_mask: u32,
        max_buckets: u32,
        work_mem: i32,
        sortopt: i32,
    ) -> Tuplesort {
        // SAFETY: lifetime erasure on the relcache tupdesc; the caller keeps
        // the index relation open for the life of the sort (hashbuild holds
        // it open across the whole build, as C does).
        let tup_desc: std::rc::Rc<TupleDescData<'static>> =
            unsafe { mem::transmute(index_rel.rd_att.clone()) };
        Self::begin_common(
            work_mem,
            sortopt,
            &[],
            false,
            None,
            SortVariant::IndexHash { tup_desc, high_mask, low_mask, max_buckets },
        )
    }

    /// `tuplesort_begin_cluster`, serial arm; keys read off the index
    /// relation as [`Tuplesort::begin_index_btree`] does (C `_bt_mkscankey`).
    pub fn begin_cluster(
        heap_tup_desc: std::rc::Rc<TupleDescData<'static>>,
        index_rel: &types_rel::Relation<'_>,
        index_attnums: &[i16],
        work_mem: i32,
        sortopt: i32,
    ) -> PgResult<Tuplesort> {
        const INDOPTION_DESC: i16 = 1 << 0;
        const INDOPTION_NULLS_FIRST: i16 = 1 << 1;
        debug_assert!(index_rel.rd_rel.relam == 403);
        let nkeys = index_rel.indnkeyatts() as usize;
        assert!(nkeys > 0 && nkeys <= index_attnums.len());
        let mut attnums = [0i16; 32];
        attnums[..nkeys].copy_from_slice(&index_attnums[..nkeys]);
        assert!(attnums[..nkeys].iter().all(|&a| a >= 0), "system-attribute index columns");
        // SAFETY: lifetime erasure as for heap_tup_desc; the caller keeps the
        // index relation open for the life of the sort.
        let index_desc: Option<std::rc::Rc<TupleDescData<'static>>> =
            if attnums[..nkeys].contains(&0) {
                Some(unsafe { mem::transmute(index_rel.rd_att.clone()) })
            } else {
                None
            };
        let mut keys = Vec::with_capacity(nkeys);
        for i in 0..nkeys {
            let indoption = index_rel.rd_indoption[i];
            let collation = index_rel.rd_indcollation[i];
            let comparator = comparator_for_index_col(
                index_rel.rd_opfamily[i],
                index_rel.rd_opcintype[i],
                collation,
            )?;
            keys.push(SortSupport {
                ssup_collation: collation,
                ssup_reverse: indoption & INDOPTION_DESC != 0,
                ssup_nulls_first: indoption & INDOPTION_NULLS_FIRST != 0,
                ssup_attno: (i + 1) as i16,
                comparator,
            });
        }
        Ok(Self::begin_common(
            work_mem,
            sortopt,
            &keys,
            false,
            None,
            SortVariant::Cluster { tup_desc: heap_tup_desc, attnums, nkeys: nkeys as u16, index_desc },
        ))
    }

    /// C divergence: as [`Tuplesort::begin_heap_with_keys`], index variant.
    #[allow(clippy::too_many_arguments)]
    pub fn begin_index_with_keys(
        tup_desc: std::rc::Rc<TupleDescData<'static>>,
        keys: &[SortSupport],
        nkeys: u16,
        enforce_unique: bool,
        unique_nulls_not_distinct: bool,
        index_name: &str,
        index_rel: Option<std::rc::Rc<types_rel::RelationData<'static>>>,
        work_mem: i32,
        sortopt: i32,
    ) -> Tuplesort {
        assert!(!keys.is_empty() && keys.len() == nkeys as usize);
        Self::begin_common(
            work_mem,
            sortopt,
            keys,
            false,
            None,
            SortVariant::Index {
                tup_desc,
                nkeys,
                enforce_unique,
                unique_nulls_not_distinct,
                index_name: std::rc::Rc::from(index_name),
                index_rel,
            },
        )
    }

    /// `tuplesort_begin_datum`; by-ref datums datumCopy into tuplecontext
    /// (cstring typlen -2 is a loud panic).
    pub fn begin_datum(
        datum_type: Oid,
        sort_operator: Oid,
        sort_collation: Oid,
        nulls_first_flag: bool,
        work_mem: i32,
        sortopt: i32,
    ) -> PgResult<Tuplesort> {
        let (typlen, typbyval) = lsyscache::get_typlenbyval(datum_type)?;
        if !typbyval && typlen < -1 {
            panic!(
                "tuplesort_begin_datum: cstring-typlen by-ref datum sort not ported \
                 for type {datum_type}"
            );
        }
        let init = SortSupportInit {
            ssup_collation: sort_collation,
            ssup_nulls_first: nulls_first_flag,
            ssup_attno: 1,
        };
        let (key, abbrev_arm) =
            ssup::prepare_sort_support_abbrev(sort_operator, &init, !typbyval)?;
        let byref_typlen = if typbyval { 0 } else { typlen };
        Ok(Self::begin_common(
            work_mem,
            sortopt,
            &[key],
            abbrev_arm.is_none(),
            abbrev_arm.map(|arm| Box::new(AbbrevState::new(arm))),
            SortVariant::Datum { byref_typlen },
        ))
    }

    /// C divergence: as [`Tuplesort::begin_heap_with_keys`], datum variant.
    pub fn begin_datum_with_key(key: SortSupport, work_mem: i32, sortopt: i32) -> Tuplesort {
        Self::begin_common(work_mem, sortopt, &[key], true, None, SortVariant::Datum { byref_typlen: 0 })
    }

    fn begin_common(
        work_mem: i32,
        sortopt: i32,
        keys: &[SortSupport],
        only_key: bool,
        abbrev: Option<Box<AbbrevState>>,
        variant: SortVariant,
    ) -> Tuplesort {
        let free_typlen = match variant {
            SortVariant::Datum { byref_typlen } => byref_typlen,
            _ => FREE_SIZE_TLEN,
        };
        let owned = McxOwned::try_new(MemoryContext::new("TupleSort main"), |mcx| {
            let allowed_mem = i64::from(work_mem.max(64)) * 1024;
            let memtuples = PgVec::with_capacity_in(INITIAL_MEMTUPSIZE, mcx);
            let mut sort_keys = PgVec::with_capacity_in(keys.len(), mcx);
            sort_keys.extend_from_slice(keys);
            let avail_mem =
                allowed_mem - (INITIAL_MEMTUPSIZE * mem::size_of::<SortTuple>()) as i64;
            Ok(TuplesortData {
                mcx,
                // C's TupleSortUseBumpTupleCxt: a bounded-capable sort
                // (TUPLESORT_ALLOWBOUNDED) pfrees tuples evicted from the
                // bounded heap (free_sort_tuple), which a bump arena cannot
                // do — its footprint would grow with INPUT rows while the
                // sort reports the bound's few kB. Mirror C exactly: aset
                // iff the bound is allowed, bump otherwise (the unbounded
                // arm keeps the bump win; reclamation there is wholesale
                // reset/end, never per-tuple).
                tuplecontext: if sortopt & TUPLESORT_ALLOWBOUNDED != 0 {
                    mcx.context().new_child("Caller tuples")
                } else {
                    mcx.context().new_child_bump("Caller tuples")
                },
                status: TupSortStatus::Initial,
                sortopt,
                bounded: false,
                bound_used: false,
                bound: 0,
                avail_mem,
                allowed_mem,
                put_watermark: 0,
                grow_memtuples: true,
                memtuples,
                current: 0,
                eof_reached: false,
                markpos_offset: 0,
                markpos_eof: false,
                max_space: 0,
                is_max_space_disk: false,
                max_space_status: TupSortStatus::Initial,
                tapes: None,
                tuple_mem: 0,
                sort_keys,
                only_key,
                have_datum1: true,
                abbrev,
                abbrev_next: 10,
                free_typlen,
                tie_track: false,
                tie_dirty: false,
                rowref_mode: false,
                rowref_missing: false,
                variant,
                unique_violation: Cell::new(None),
            })
        })
        .expect("TupleSort main context construction is infallible");
        Tuplesort(owned)
    }

    pub fn set_bound(&mut self, bound: i64) {
        self.0.with_mut(|st| {
            debug_assert!(
                !matches!(st.variant, SortVariant::Index { .. }),
                "bounded index sorts do not exist (tuplesortvariants.c)"
            );
            debug_assert!(st.status == TupSortStatus::Initial && st.memtuples.is_empty());
            debug_assert!(st.sortopt & TUPLESORT_ALLOWBOUNDED != 0);
            debug_assert!(!st.bounded);
            if bound > i64::from(i32::MAX / 2) {
                return;
            }
            st.bounded = true;
            st.bound = bound as i32;
            // C: bounded sorts are not an effective target for abbreviation —
            // disarm and restore the authoritative comparator (measured here
            // too: top-100/200k text paid ~142 instr/put of conversion for
            // discard-after-one-compare work; docs/optimizations/abbrev-keys.md).
            if let Some(abbrev) = st.abbrev.take() {
                st.sort_keys[0].comparator = abbrev.full_comparator;
            }
            st.recompute_put_watermark();
        })
    }

    pub fn used_bound(&self) -> bool {
        self.0.with(|st| st.bound_used)
    }

    pub fn get_stats(&mut self) -> TuplesortInstrumentation {
        self.0.with_mut(|st| {
            st.updatemax();
            TuplesortInstrumentation {
                sortMethod: match st.max_space_status {
                    TupSortStatus::SortedInMem if st.bound_used => TuplesortMethod::TopNHeapsort,
                    TupSortStatus::SortedInMem => TuplesortMethod::Quicksort,
                    TupSortStatus::SortedOnTape => TuplesortMethod::ExternalSort,
                    TupSortStatus::FinalMerge => TuplesortMethod::ExternalMerge,
                    TupSortStatus::Initial
                    | TupSortStatus::Bounded
                    | TupSortStatus::BuildRuns => TuplesortMethod::StillInProgress,
                },
                spaceType: if st.is_max_space_disk {
                    TuplesortSpaceType::Disk
                } else {
                    TuplesortSpaceType::Memory
                },
                spaceUsed: (st.max_space + 1023) / 1024,
            }
        })
    }

    /// Streaming top-k cutoff boundary (lane-v2 sort-feed pre-filter; no C
    /// counterpart — a pure read of bounded-heap state). While the bounded
    /// heap is full (`TSS_BOUNDED`, entered at `make_bounded_heap`), the heap
    /// root `memtuples[0]` is the WORST surviving top-k member under the
    /// reversed comparator — the current k-th boundary. Returns its leading
    /// sort-key datum (`datum1`, authentic: `set_bound` disarms abbreviation)
    /// and null flag; `None` outside `TSS_BOUNDED` (heap not yet full, or not
    /// a bounded sort).
    ///
    /// SOUND FOR BY-VALUE LEADING KEYS ONLY: for by-ref keys `datum1` points
    /// into the root's stored tuple, which is freed the moment a later put
    /// evicts it (`puttuple_bounded_replace`). The lane admits only by-value
    /// kernel families, so the returned Datum is a plain value copy.
    pub fn topk_boundary(&self) -> Option<(Datum, bool)> {
        self.0.with(|st| {
            if st.status != TupSortStatus::Bounded {
                return None;
            }
            debug_assert!(st.bounded && st.have_datum1 && st.abbrev.is_none());
            let root = st.memtuples.first()?;
            Some((root.datum1, root.isnull1))
        })
    }

    /// Arm top-k boundary-tie tracking (lane zone-adaptive sort feeds only;
    /// see the `tie_track` field). Must be armed before the first put.
    pub fn arm_topk_tie_track(&mut self) {
        self.0.with_mut(|st| {
            debug_assert!(st.status == TupSortStatus::Initial && st.memtuples.is_empty());
            st.tie_track = true;
            st.tie_dirty = false;
        })
    }

    /// After `performsort`, with tie tracking armed: could the SELECTION or
    /// ORDER of the first `bound` output rows depend on input arrival order?
    /// True iff (a) a full-key tie group at the final k-th boundary extends
    /// beyond the heap (`tie_dirty`, maintained by the bounded put paths;
    /// also the never-bounded cut pair), or (b) any adjacent full-key-equal
    /// pair exists within the first `bound` output rows (the in-memory qsort
    /// and the bounded heapsort are both unstable, so retained-tie emit
    /// order is arrival-dependent). Conservative `true` when the sort left
    /// memory (spilled bounded feeds are out of the lane's admitted
    /// envelope). False whenever tracking was never armed.
    pub fn topk_tie_ambiguous(&self) -> bool {
        self.topk_tie_ambiguity().is_some()
    }

    /// Arm the top-k rowref total order (tie-ordering rule 2; see the
    /// `rowref_mode` field). Heap-variant bounded sorts only; every put must
    /// then go through `puttupleslot_rowref`. Must be armed before the first
    /// put; mutually exclusive with `arm_topk_tie_track`.
    pub fn arm_topk_rowref(&mut self) {
        self.0.with_mut(|st| {
            debug_assert!(st.status == TupSortStatus::Initial && st.memtuples.is_empty());
            debug_assert!(!st.tie_track);
            debug_assert!(matches!(st.variant, SortVariant::Heap { .. }));
            st.rowref_mode = true;
            st.rowref_missing = false;
        })
    }

    /// `topk_tie_ambiguous` with the trigger distinguished: `CutSelection` =
    /// which rows made the LIMIT cut is arrival-dependent (demotion is the
    /// only byte-exact answer); `RetainedOrder` = the cut set is exact but
    /// equal-full-key rows inside the output can emit in arrival-dependent
    /// order (the surface the ratified tie-order relaxation covers).
    /// `CutSelection` dominates when both hold.
    ///
    /// Rowref mode (rule 2): the (key, rowref) total order has no ties, so
    /// selection AND retained order are exact by construction — `None` —
    /// unless the contract broke: a put carried no rowref, the sort left
    /// memory (rowrefs are not honored by the tape merge), or the bounded
    /// transition never ran while more rows than the bound survived (the
    /// cut would then be taken by the consumer over an unwrapped in-memory
    /// sort). All three report `CutSelection` so the consumer demotes.
    pub fn topk_tie_ambiguity(&self) -> Option<TopkTieAmbiguity> {
        self.0.with(|st| {
            if st.rowref_mode {
                let exact = !st.rowref_missing
                    && st.status == TupSortStatus::SortedInMem
                    && (st.bound_used || st.memtuples.len() <= st.bound as usize);
                return if exact { None } else { Some(TopkTieAmbiguity::CutSelection) };
            }
            if !st.tie_track {
                return None;
            }
            if st.tie_dirty || st.status != TupSortStatus::SortedInMem {
                return Some(TopkTieAmbiguity::CutSelection);
            }
            debug_assert!(st.bounded && st.abbrev.is_none());
            let len = st.memtuples.len();
            // Pairs (i, i+1) for i < min(bound, len-1): every adjacent pair
            // inside the emitted prefix, plus the cut pair when more rows
            // than the bound survived in memory (the never-bounded case,
            // where `tie_dirty` never armed — a cut-pair tie is selection
            // ambiguity there, interior pairs are order-only).
            let bound = st.bound as usize;
            let hi = bound.min(len.saturating_sub(1));
            let ctx = ctx!(st);
            dispatch_cmp!(ctx, |cmp| {
                let mut kind = None;
                for i in 0..hi {
                    if cmp(&st.memtuples[i], &st.memtuples[i + 1]) == 0 {
                        if i + 1 == bound {
                            kind = Some(TopkTieAmbiguity::CutSelection);
                            break;
                        }
                        kind = Some(TopkTieAmbiguity::RetainedOrder);
                    }
                }
                kind
            })
        })
    }

    /// `tuplesort_reset`: recycle the batch, keep keys + memtuples capacity.
    pub fn reset(&mut self) {
        self.0.with_mut(|st| {
            st.updatemax();
            if let Some(ts) = st.tapes.take() {
                ts.tapeset
                    .close()
                    .expect("tuplesort_reset: closing tape temp files failed");
            }
            st.reset_tuplecontext();
            st.memtuples.clear();
            if st.memtuples.capacity() == 0 {
                st.memtuples.reserve(INITIAL_MEMTUPSIZE);
            }
            st.tuple_mem = 0;
            st.status = TupSortStatus::Initial;
            st.bounded = false;
            st.bound_used = false;
            st.bound = 0;
            st.grow_memtuples = true;
            st.current = 0;
            st.eof_reached = false;
            st.markpos_offset = 0;
            st.markpos_eof = false;
            // C's reset leaves availMem = allowedMem (memtuples not re-charged).
            st.avail_mem = st.allowed_mem;
            st.tie_track = false;
            st.tie_dirty = false;
            st.rowref_mode = false;
            st.rowref_missing = false;
            st.abbrev_next = 10;
            st.recompute_put_watermark();
        })
    }

    #[inline]
    pub fn puttupleslot<'q>(
        &mut self,
        slot: &mut SlotData<'q>,
        slot_mcx: Mcx<'q>,
    ) -> PgResult<()> {
        self.puttupleslot_inner(slot, slot_mcx, None)
    }

    /// `puttupleslot` carrying the row's 48-bit physical rowref (rowref mode,
    /// tie-ordering rule 2): the rowref is stamped into the copied minimal
    /// tuple's `mt_padding` (bytes 4..10 of the image — pure padding in C and
    /// in the port; never read by deform, never part of the data bytes), from
    /// where the bounded-heap tie-break comparisons read it.
    #[inline]
    pub fn puttupleslot_rowref<'q>(
        &mut self,
        slot: &mut SlotData<'q>,
        slot_mcx: Mcx<'q>,
        rowref: u64,
    ) -> PgResult<()> {
        self.puttupleslot_inner(slot, slot_mcx, Some(rowref))
    }

    #[inline]
    fn puttupleslot_inner<'q>(
        &mut self,
        slot: &mut SlotData<'q>,
        slot_mcx: Mcx<'q>,
        rowref: Option<u64>,
    ) -> PgResult<()> {
        self.0.with_mut(|st| {
            let mtup = exectuples::exec_copy_slot_minimal_tuple(
                slot,
                slot_mcx,
                st.tuplecontext.mcx(),
                0,
            )?;
            let t_len = mtup.t_len() as usize;
            let tuple = mtup.as_ptr().cast_mut().cast::<MinimalTupleData>();
            // Ownership moves to tuplecontext (bulk-freed at end); the wrapper
            // must not run its deallocating Drop.
            mem::forget(mtup);

            match rowref {
                Some(rr) => {
                    debug_assert!(rr >> 48 == 0, "rowref exceeds the 48-bit padding");
                    // SAFETY: fresh live image, >= the 15-byte minimal-tuple
                    // header; bytes 4..10 are mt_padding (see MinimalTupleData).
                    unsafe {
                        let p = tuple.cast::<u8>();
                        p.add(4).cast::<u32>().write_unaligned(rr as u32);
                        p.add(8).cast::<u16>().write_unaligned((rr >> 32) as u16);
                    }
                }
                // Rowref-armed sorts require every put to carry one; a bare
                // put leaves garbage padding, so record the contract break
                // (the consumer demotes on `topk_tie_ambiguity`).
                None => st.rowref_missing |= st.rowref_mode,
            }

            let SortVariant::Heap { tup_desc } = &st.variant else {
                panic!("tuplesort_puttupleslot on a non-heap tuplesort")
            };
            let mut isnull1 = false;
            // SAFETY: fresh live copy formed under the slot's descriptor,
            // which matches tup_desc (nodeSort contract).
            let datum1 = unsafe {
                minimal_getattr(tuple, st.sort_keys[0].ssup_attno as i32, tup_desc, &mut isnull1)
            };
            st.puttuple_common(tuple, datum1, isnull1, maxalign(t_len) as i64)
        })
    }

    /// Heap-variant put straight from deformed values (the pgrcolumnar ingest-sort
    /// seam; no slot exists on that path).
    pub fn putvalues(&mut self, values: &[Datum], isnull: &[bool]) -> PgResult<()> {
        self.0.with_mut(|st| {
            let SortVariant::Heap { tup_desc } = &st.variant else {
                panic!("tuplesort_putvalues on a non-heap tuplesort")
            };
            let mtup = heaptuple::heap_form_minimal_tuple(
                st.tuplecontext.mcx(),
                tup_desc,
                values,
                isnull,
                0,
            )?;
            let t_len = mtup.t_len() as usize;
            let tuple = mtup.as_ptr().cast_mut().cast::<MinimalTupleData>();
            // Ownership moves to tuplecontext (bulk-freed at end).
            mem::forget(mtup);
            // Rowref-armed sorts require rowref-carrying puts (see
            // `puttupleslot_inner`); this seam never carries one.
            st.rowref_missing |= st.rowref_mode;
            let mut isnull1 = false;
            // SAFETY: fresh live image formed under tup_desc just above.
            let datum1 = unsafe {
                minimal_getattr(tuple, st.sort_keys[0].ssup_attno as i32, tup_desc, &mut isnull1)
            };
            st.puttuple_common(tuple, datum1, isnull1, maxalign(t_len) as i64)
        })
    }

    /// Heap-variant get into deformed-value buffers (len >= the descriptor's
    /// natts). By-ref datums point into sort-owned memory, live until the
    /// next call. false = drained.
    pub fn getvalues(
        &mut self,
        forward: bool,
        values: &mut [Datum],
        isnull: &mut [bool],
    ) -> PgResult<bool> {
        self.0.with_mut(|st| {
            let Some(stup) = st.gettuple_common(forward)? else {
                return Ok(false);
            };
            let SortVariant::Heap { tup_desc } = &st.variant else {
                panic!("tuplesort_getvalues on a non-heap tuplesort")
            };
            debug_assert!(!stup.tuple.is_null());
            // SAFETY: live sort-owned minimal-tuple image under tup_desc.
            unsafe { mgetattr::minimal_deform(stup.tuple, tup_desc, values, isnull) };
            Ok(true)
        })
    }

    /// `tuplesort_putindextuplevalues`.
    #[inline]
    pub fn putindextuplevalues(
        &mut self,
        self_tid: ItemPointerData,
        values: &[Datum],
        isnull: &[bool],
    ) -> PgResult<()> {
        self.0.with_mut(|st| {
            let (SortVariant::Index { tup_desc, .. } | SortVariant::IndexHash { tup_desc, .. }) =
                &st.variant
            else {
                panic!("tuplesort_putindextuplevalues on a non-index tuplesort")
            };
            let mut buf =
                nbtree::itup::index_form_tuple(st.tuplecontext.mcx(), tup_desc, values, isnull)?;
            let tuplen = buf.size() as i64;
            // SAFETY: t_tid = first 6 bytes of the owned image (itup.h).
            unsafe {
                buf.as_mut_ptr()
                    .cast::<ItemPointerData>()
                    .write_unaligned(self_tid);
            }
            let tuple = buf.as_mut_ptr();
            // Ownership moves to tuplecontext (bulk-freed at end).
            mem::forget(buf);

            let mut isnull1 = false;
            // SAFETY: freshly formed live image under tup_desc.
            let datum1 =
                unsafe { nbtree::itup::index_getattr(tuple, 1, tup_desc, &mut isnull1) };
            st.puttuple_common(tuple.cast::<MinimalTupleData>(), datum1, isnull1, tuplen)
        })
    }

    /// Parallel index-build feed (M4.2): put one PRE-FORMED index-tuple
    /// image (`t_tid` already set — a pool worker formed it under a
    /// descriptor identical to this sort's). Byte-equivalent to
    /// [`Tuplesort::putindextuplevalues`] modulo WHO ran `index_form_tuple`:
    /// the image is copied into `tuplecontext` and enters through the same
    /// `puttuple_common` tail (datum1 extraction included), so the sorted
    /// output — and every downstream page image — is independent of which
    /// entry point fed the sort.
    pub fn put_index_tuple_image(&mut self, image: &[u8]) -> PgResult<()> {
        self.0.with_mut(|st| {
            let (SortVariant::Index { tup_desc, .. } | SortVariant::IndexHash { tup_desc, .. }) =
                &st.variant
            else {
                panic!("put_index_tuple_image on a non-index tuplesort")
            };
            let tuplen = image.len() as i64;
            // MAXALIGNed backing store (u64 words), as putheaptuple does:
            // index_getattr walks the copy, not the caller's bytes.
            let words = image.len().div_ceil(8);
            let mut blob: PgVec<'_, u64> =
                ::mcx::vec_with_capacity_in(st.tuplecontext.mcx(), words)?;
            blob.resize(words, 0);
            let tuple = blob.as_mut_ptr().cast::<u8>();
            // SAFETY: fresh words*8 >= image.len() byte buffer.
            unsafe {
                core::ptr::copy_nonoverlapping(image.as_ptr(), tuple, image.len());
            }
            // Ownership moves to tuplecontext (bulk-freed at end).
            mem::forget(blob);

            let mut isnull1 = false;
            // SAFETY: freshly copied live image under tup_desc.
            let datum1 =
                unsafe { nbtree::itup::index_getattr(tuple, 1, tup_desc, &mut isnull1) };
            st.puttuple_common(tuple.cast::<MinimalTupleData>(), datum1, isnull1, tuplen)
        })
    }

    /// `tuplesort_putheaptuple` (cluster variant). `itup` carries the formed
    /// index key tuple image; required iff the expression-index lane is armed.
    pub fn putheaptuple(
        &mut self,
        tup: &::types_tuple::htup::HeapTupleData<'_>,
        itup: Option<&[u8]>,
    ) -> PgResult<()> {
        self.0.with_mut(|st| {
            let SortVariant::Cluster { tup_desc, attnums, index_desc, .. } = &st.variant else {
                panic!("tuplesort_putheaptuple on a non-cluster tuplesort")
            };
            assert!(itup.is_some() == index_desc.is_some());
            let t_len = tup.t_len as usize;
            let itup_len = itup.map_or(0, |i| i.len());
            let itup_off = maxalign(16 + t_len);
            let words = (itup_off + itup_len).div_ceil(8);
            let mut blob: PgVec<'_, u64> =
                ::mcx::vec_with_capacity_in(st.tuplecontext.mcx(), words)?;
            blob.resize(words, 0);
            let base = blob.as_mut_ptr().cast::<u8>();
            // SAFETY: fresh words*8 >= itup_off+itup_len byte buffer; source
            // image is live for t_len bytes (HeapTupleData invariant).
            unsafe {
                let hdr = base.cast::<ClusterTupleHeader>();
                (*hdr).t_len = tup.t_len;
                (*hdr).blk = ::types_tuple::ItemPointerGetBlockNumberNoCheck(&tup.t_self);
                (*hdr).pos = tup.t_self.ip_posid;
                (*hdr).itup_len = itup_len as u32;
                core::ptr::copy_nonoverlapping(tup.header_ptr(), base.add(16), t_len);
                if let Some(itup) = itup {
                    core::ptr::copy_nonoverlapping(itup.as_ptr(), base.add(itup_off), itup_len);
                }
            }
            mem::forget(blob);

            // SAFETY: image copied above under the heap descriptor.
            let stored = unsafe {
                ::types_tuple::htup::HeapTupleData::from_raw_parts(
                    base.add(16),
                    tup.t_len,
                    tup.t_self,
                    tup.t_tableOid,
                )
            };
            let mut isnull1 = false;
            // SAFETY: live images; attnums[0] is a valid user attno on the
            // non-expression lane.
            let datum1 = unsafe {
                match index_desc {
                    Some(idesc) => nbtree::itup::index_getattr(
                        base.add(itup_off).cast_const().cast(),
                        1,
                        idesc,
                        &mut isnull1,
                    ),
                    None => ::types_tuple::heap_getattr(
                        &stored,
                        attnums[0] as i32,
                        tup_desc,
                        &mut isnull1,
                    ),
                }
            };
            st.puttuple_common(
                base.cast::<MinimalTupleData>(),
                datum1,
                isnull1,
                (itup_off + maxalign(itup_len)) as i64,
            )
        })
    }

    /// `tuplesort_getheaptuple`; image owned by the sort, valid until the
    /// next tuplesort call (caller contract, as C's shouldFree=false).
    pub fn getheaptuple(
        &mut self,
        forward: bool,
    ) -> PgResult<Option<::types_tuple::htup::HeapTupleData<'static>>> {
        self.0.with_mut(|st| {
            debug_assert!(matches!(st.variant, SortVariant::Cluster { .. }));
            Ok(st.gettuple_common(forward)?.map(|stup| {
                let base = stup.tuple.cast_const().cast::<u8>();
                // SAFETY: blob written by putheaptuple; lives until the sort
                // is reset/ended.
                unsafe {
                    let hdr = base.cast::<ClusterTupleHeader>();
                    ::types_tuple::htup::HeapTupleData::from_raw_parts(
                        base.add(16),
                        (*hdr).t_len,
                        ::types_tuple::ItemPointerData::new((*hdr).blk, (*hdr).pos),
                        ::types_core::InvalidOid,
                    )
                }
            }))
        })
    }

    /// _bt_load's spool/spool2 merge comparator (nbtsort.c:1203): full key
    /// compare via this sort's own SortSupport keys, then the TID tiebreak.
    /// No unique enforcement — the merge legitimately interleaves equal keys
    /// from the dead-tuple spool.
    ///
    /// # Safety
    /// `a` and `b` are live index-tuple images formed under this sort's
    /// tuple descriptor.
    pub unsafe fn compare_index_tuples(&self, a: nbtree::itup::ITup, b: nbtree::itup::ITup) -> i32 {
        self.0.with(|st| {
            let SortVariant::Index { tup_desc, nkeys, .. } = &st.variant else {
                panic!("compare_index_tuples on a non-index tuplesort")
            };
            for nkey in 1..=(*nkeys as i16) {
                let key = &st.sort_keys[nkey as usize - 1];
                let (mut isnull1, mut isnull2) = (false, false);
                // SAFETY: live images under this sort's descriptor (fn contract).
                let (d1, d2) = unsafe {
                    (
                        nbtree::itup::index_getattr(a, nkey, tup_desc, &mut isnull1),
                        nbtree::itup::index_getattr(b, nkey, tup_desc, &mut isnull2),
                    )
                };
                let c = ssup::apply_sort_comparator_in(st.mcx, d1, isnull1, d2, isnull2, key);
                if c != 0 {
                    return c;
                }
            }
            // SAFETY: t_tid header read of live images (fn contract).
            let (t1, t2) = unsafe { (nbtree::itup::t_tid(a), nbtree::itup::t_tid(b)) };
            ::types_tuple::itemptr::ItemPointerCompare(&t1, &t2)
        })
    }

    /// `tuplesort_getindextuple`; image owned by the sort, valid until the
    /// next tuplesort call.
    #[inline]
    pub fn getindextuple(&mut self, forward: bool) -> PgResult<Option<nbtree::itup::ITup>> {
        self.0.with_mut(|st| {
            debug_assert!(matches!(
                st.variant,
                SortVariant::Index { .. } | SortVariant::IndexHash { .. }
            ));
            Ok(st
                .gettuple_common(forward)?
                .map(|stup| stup.tuple.cast_const().cast::<u8>()))
        })
    }

    #[inline]
    pub fn putdatum(&mut self, val: Datum, is_null: bool) -> PgResult<()> {
        self.0.with_mut(|st| {
            let SortVariant::Datum { byref_typlen } = st.variant else {
                panic!("tuplesort_putdatum on a non-datum tuplesort")
            };
            if is_null || byref_typlen == 0 {
                let datum1 = if is_null { Datum::null() } else { val };
                return st.puttuple_common(core::ptr::null_mut(), datum1, is_null, 0);
            }
            // C datumCopy: the copy is canonical, valid until reset/end.
            // Expanded datums flatten; every other form — on-disk toast
            // pointers included — copies verbatim, and the varlena
            // comparators detoast per comparison as C's fastcmps do.
            let mut src = val.as_usize() as *const u8;
            let mut _flat_scratch = None;
            // SAFETY: a non-null by-ref datum is readable for its full size.
            let size = unsafe {
                if byref_typlen == -1 {
                    if ::types_tuple::varatt::varatt_is_1b_e(src)
                        && ::types_tuple::varatt::vartag_is_expanded(*src.add(1))
                    {
                        let raw = core::slice::from_raw_parts(
                            src,
                            ::types_tuple::varatt::varsize_any(src),
                        );
                        let scratch = ::mcx::MemoryContext::new_bump("putdatum flatten");
                        let img = ::detoast_seams::detoast_attr::call(scratch.mcx(), raw)?;
                        let n = img.len();
                        src = img.leak().as_ptr();
                        _flat_scratch = Some(scratch);
                        n
                    } else {
                        ::types_tuple::varatt::varsize_any(src)
                    }
                } else {
                    byref_typlen as usize
                }
            };
            let tmcx = st.tuplecontext.mcx();
            let layout = core::alloc::Layout::from_size_align(size, 8).expect("putdatum layout");
            let dst: core::ptr::NonNull<u8> = ::mcx::Allocator::allocate(&tmcx, layout)
                .map_err(|_| tmcx.oom(size))?
                .cast();
            // SAFETY: fresh size-byte allocation; src readable per above.
            unsafe { core::ptr::copy_nonoverlapping(src, dst.as_ptr(), size) };
            let datum1 = Datum::from_usize(dst.as_ptr() as usize);
            st.puttuple_common(
                dst.as_ptr().cast::<MinimalTupleData>(),
                datum1,
                false,
                maxalign(size) as i64,
            )
        })
    }

    #[inline]
    pub fn datum_sort_is_byref(&self) -> bool {
        self.0
            .with(|st| matches!(st.variant, SortVariant::Datum { byref_typlen } if byref_typlen != 0))
    }

    /// 0 for by-value datum sorts, else the sorted type's typlen.
    #[inline]
    pub fn datum_byref_typlen(&self) -> i16 {
        self.0.with(|st| match st.variant {
            SortVariant::Datum { byref_typlen } => byref_typlen,
            _ => panic!("datum_byref_typlen on a non-datum tuplesort"),
        })
    }

    /// True once the sort went external. Returned by-ref values then live in
    /// recycled slab slots (valid only until the next fetch, as C's
    /// copy=false), unlike in-memory sorts whose images live until reset/end.
    #[inline]
    pub fn spilled(&self) -> bool {
        self.0.with(|st| st.tapes.is_some())
    }

    /// C divergence (structural lever): batched putdatum — the per-call len
    /// memory round-trip is ~43 cyc/put on V2 (docs/benchmarks/tuplesort.md).
    #[inline]
    pub fn putdatum_batch<R>(
        &mut self,
        f: impl for<'a, 'm> FnOnce(&mut DatumPutter<'a, 'm>) -> PgResult<R>,
    ) -> PgResult<R> {
        self.0.with_mut(|st| {
            // The batch putter parks raw pointers — by-ref needs putdatum's copy.
            assert!(
                matches!(st.variant, SortVariant::Datum { byref_typlen: 0 }),
                "tuplesort_putdatum_batch: by-ref datum sort requires putdatum"
            );
            let mut putter = DatumPutter::new(st);
            let result = f(&mut putter);
            putter.flush();
            result
        })
    }

    #[inline]
    pub fn performsort(&mut self) -> PgResult<()> {
        self.0.with_mut(|st| {
            match st.status {
                TupSortStatus::Initial => {
                    st.sort_memtuples()?;
                    st.status = TupSortStatus::SortedInMem;
                }
                TupSortStatus::Bounded => st.sort_bounded_heap()?,
                TupSortStatus::BuildRuns => {
                    st.dumptuples(true)?;
                    st.mergeruns()?;
                }
                TupSortStatus::SortedInMem
                | TupSortStatus::SortedOnTape
                | TupSortStatus::FinalMerge => {
                    return Err(invalid_state("tuplesort_performsort"))
                }
            }
            st.put_watermark = 0;
            st.current = 0;
            st.eof_reached = false;
            if let Some(ts) = st.tapes.as_mut() {
                ts.markpos_block = 0;
            }
            st.markpos_offset = 0;
            st.markpos_eof = false;
            Ok(())
        })
    }

    /// `tuplesort_gettupleslot`; `abbrev` out-param elided (no caller uses
    /// the cheap-inequality hint yet).
    #[inline]
    pub fn gettupleslot<'q>(
        &mut self,
        forward: bool,
        copy: bool,
        slot: &mut SlotData<'q>,
        slot_mcx: Mcx<'q>,
    ) -> PgResult<bool> {
        self.0.with_mut(|st| {
            let Some(stup) = st.gettuple_common(forward)? else {
                exectuples::exec_clear_tuple(slot, slot_mcx);
                return Ok(false);
            };
            debug_assert!(!stup.tuple.is_null());
            if copy {
                // SAFETY: stup.tuple is a live tuplecontext image of t_len bytes.
                let bytes = unsafe {
                    core::slice::from_raw_parts(
                        stup.tuple.cast_const().cast::<u8>(),
                        (*stup.tuple).t_len as usize,
                    )
                };
                let owned = heaptuple::heap_copy_minimal_tuple(slot_mcx, bytes, 0)?;
                exectuples::exec_store_minimal_tuple_owned(slot, slot_mcx, owned);
            } else {
                // SAFETY: whole-image pointer, live until the tuplesort is
                // reset/ended, as C's shouldFree=false store (caller contract;
                // nodeSort clears the slot before dropping the sort).
                unsafe {
                    exectuples::exec_store_minimal_tuple_ptr(
                        slot,
                        slot_mcx,
                        core::ptr::NonNull::new_unchecked(stup.tuple),
                    );
                }
            }
            Ok(true)
        })
    }

    #[inline]
    pub fn getdatum(&mut self, forward: bool) -> PgResult<Option<NullableDatum>> {
        self.0.with_mut(|st| {
            debug_assert!(matches!(st.variant, SortVariant::Datum { .. }));
            let abbrev_armed = st.abbrev.is_some();
            Ok(st.gettuple_common(forward)?.map(|stup| NullableDatum {
                // Armed abbrev: datum1 is the converted word; the datumCopy
                // image in `tuple` is the original.
                value: if abbrev_armed && !stup.isnull1 {
                    Datum::from_usize(stup.tuple as usize)
                } else {
                    stup.datum1
                },
                isnull: stup.isnull1,
            }))
        })
    }

    /// `tuplesort_getdatum` with the abbreviated-key out-param: the second
    /// word is the converted datum1 iff abbreviation is armed, else 0 (C
    /// leaves the caller's initial value untouched).
    #[inline]
    pub fn getdatum_abbrev(
        &mut self,
        forward: bool,
    ) -> PgResult<Option<(NullableDatum, Datum)>> {
        self.0.with_mut(|st| {
            debug_assert!(matches!(st.variant, SortVariant::Datum { .. }));
            let abbrev_armed = st.abbrev.is_some();
            Ok(st.gettuple_common(forward)?.map(|stup| {
                let nd = NullableDatum {
                    value: if abbrev_armed && !stup.isnull1 {
                        Datum::from_usize(stup.tuple as usize)
                    } else {
                        stup.datum1
                    },
                    isnull: stup.isnull1,
                };
                let abbrev = if abbrev_armed && !stup.isnull1 {
                    stup.datum1
                } else {
                    Datum::null()
                };
                (nd, abbrev)
            }))
        })
    }

    /// `tuplesort_skiptuples`, forward-only (the C backward arm needs
    /// random access and has no in-tree caller).
    pub fn skiptuples(&mut self, ntuples: i64, forward: bool) -> PgResult<bool> {
        assert!(forward, "tuplesort_skiptuples: backward skip not ported");
        if ntuples < 0 {
            return Ok(false);
        }
        self.0.with_mut(|st| match st.status {
            TupSortStatus::SortedInMem => {
                if st.memtuples.len() - st.current >= ntuples as usize {
                    st.current += ntuples as usize;
                    return Ok(true);
                }
                st.current = st.memtuples.len();
                st.eof_reached = true;
                Ok(false)
            }
            TupSortStatus::SortedOnTape | TupSortStatus::FinalMerge => {
                for _ in 0..ntuples {
                    if st.gettuple_common(true)?.is_none() {
                        return Ok(false);
                    }
                    cfi()?;
                }
                Ok(true)
            }
            _ => Err(invalid_state("tuplesort_skiptuples")),
        })
    }

    pub fn rescan(&mut self) -> PgResult<()> {
        self.0.with_mut(|st| {
            debug_assert!(st.sortopt & TUPLESORT_RANDOMACCESS != 0);
            match st.status {
                TupSortStatus::SortedInMem => {
                    st.current = 0;
                    st.eof_reached = false;
                    st.markpos_offset = 0;
                    st.markpos_eof = false;
                    Ok(())
                }
                TupSortStatus::SortedOnTape => {
                    let ts = st.tapes.as_mut().expect("SortedOnTape without tapes");
                    let tape = ts.result_tape.expect("SortedOnTape without result tape");
                    ts.tapeset.rewind_for_read(tape, 0)?;
                    ts.markpos_block = 0;
                    st.eof_reached = false;
                    st.markpos_offset = 0;
                    st.markpos_eof = false;
                    Ok(())
                }
                _ => Err(invalid_state("tuplesort_rescan")),
            }
        })
    }

    pub fn markpos(&mut self) -> PgResult<()> {
        self.0.with_mut(|st| {
            debug_assert!(st.sortopt & TUPLESORT_RANDOMACCESS != 0);
            match st.status {
                TupSortStatus::SortedInMem => {
                    st.markpos_offset = st.current;
                    st.markpos_eof = st.eof_reached;
                    Ok(())
                }
                TupSortStatus::SortedOnTape => {
                    let ts = st.tapes.as_mut().expect("SortedOnTape without tapes");
                    let tape = ts.result_tape.expect("SortedOnTape without result tape");
                    let (block, offset) = ts.tapeset.tell(tape)?;
                    ts.markpos_block = block;
                    st.markpos_offset = offset as usize;
                    st.markpos_eof = st.eof_reached;
                    Ok(())
                }
                _ => Err(invalid_state("tuplesort_markpos")),
            }
        })
    }

    pub fn restorepos(&mut self) -> PgResult<()> {
        self.0.with_mut(|st| {
            debug_assert!(st.sortopt & TUPLESORT_RANDOMACCESS != 0);
            match st.status {
                TupSortStatus::SortedInMem => {
                    st.current = st.markpos_offset;
                    st.eof_reached = st.markpos_eof;
                    Ok(())
                }
                TupSortStatus::SortedOnTape => {
                    let ts = st.tapes.as_mut().expect("SortedOnTape without tapes");
                    let tape = ts.result_tape.expect("SortedOnTape without result tape");
                    ts.tapeset.seek(tape, ts.markpos_block, st.markpos_offset as i32)?;
                    st.eof_reached = st.markpos_eof;
                    Ok(())
                }
                _ => Err(invalid_state("tuplesort_restorepos")),
            }
        })
    }

    pub fn end(self) {}

    /// Test-only: the caller-tuples context's stats (kind + real arena
    /// footprint) — pins the bounded-arm aset choice and that eviction
    /// frees physically (footprint tracks the bound, not the input).
    #[cfg(test)]
    pub(crate) fn tuplecontext_stats(&mut self) -> ::mcx::TreeStats {
        self.0.with_mut(|st| st.tuplecontext.stats_tree())
    }
}

/// Register-resident put cursor over the TSS_INITIAL window [len, watermark).
/// Perf constraint: the slow leg travels BY VALUE through `datum_put_slow`;
/// `&mut self` into an outlined callee forces next/stop back into memory.
pub struct DatumPutter<'a, 'm> {
    st: &'a mut TuplesortData<'m>,
    next: *mut SortTuple,
    stop: *mut SortTuple,
}

impl<'a, 'm> DatumPutter<'a, 'm> {
    #[inline]
    fn new(st: &'a mut TuplesortData<'m>) -> Self {
        let (next, stop) = datum_put_window(st);
        DatumPutter { st, next, stop }
    }

    #[inline(always)]
    pub fn put(&mut self, val: Datum, is_null: bool) -> PgResult<()> {
        let next = self.next;
        if next >= self.stop {
            let (next, stop) = datum_put_slow(self.st, next, val, is_null)?;
            self.next = next;
            self.stop = stop;
            return Ok(());
        }
        let datum1 = if is_null { Datum::null() } else { val };
        // SAFETY: next < stop = base + put_watermark <= base + capacity - 1
        // (recompute_put_watermark invariant).
        unsafe {
            core::ptr::write(
                next,
                SortTuple { tuple: core::ptr::null_mut(), datum1, isnull1: is_null },
            );
            self.next = next.add(1);
        }
        Ok(())
    }

    #[inline]
    fn flush(&mut self) {
        datum_put_flush(self.st, self.next);
    }
}

#[inline]
fn datum_put_window<'m>(st: &mut TuplesortData<'m>) -> (*mut SortTuple, *mut SortTuple) {
    let base = st.memtuples.as_mut_ptr();
    // SAFETY: len <= capacity and put_watermark <= capacity - 1.
    unsafe { (base.add(st.memtuples.len()), base.add(st.put_watermark as usize)) }
}

fn datum_put_flush<'m>(st: &mut TuplesortData<'m>, next: *mut SortTuple) {
    let base = st.memtuples.as_mut_ptr();
    // SAFETY: next derives from base by in-bounds adds; all below it written.
    unsafe {
        let len = next.offset_from(base) as usize;
        debug_assert!(len <= st.memtuples.capacity());
        st.memtuples.set_len(len);
    }
}

#[inline(never)]
fn datum_put_slow<'m>(
    st: &mut TuplesortData<'m>,
    next: *mut SortTuple,
    val: Datum,
    is_null: bool,
) -> PgResult<(*mut SortTuple, *mut SortTuple)> {
    let datum1 = if is_null { Datum::null() } else { val };
    // Bounded: window permanently empty, len pinned at bound — flush is a
    // no-op and the discard leg runs inline (C's one-comparetup-per-put
    // shape). Caller is putdatum_batch: byval Datum asserted, abbrev
    // disarmed by set_bound ⇒ datum1 decides alone, no image to free.
    if st.status == TupSortStatus::Bounded {
        // SAFETY: TSS_BOUNDED holds exactly `bound` >= 1 tuples; Datum
        // variant carries a sort key (begin_datum asserts).
        let (heap_top, key0) = unsafe {
            (*st.memtuples.get_unchecked(0), st.sort_keys.get_unchecked(0))
        };
        debug_assert!(st.abbrev.is_none());
        debug_assert!(matches!(st.variant, SortVariant::Datum { byref_typlen: 0 }));
        let compare = ssup::apply_sort_comparator_as_in(
            key0.comparator,
            st.mcx,
            datum1,
            is_null,
            heap_top.datum1,
            heap_top.isnull1,
            key0,
        );
        if compare <= 0 {
            cfi()?;
        } else {
            st.puttuple_bounded_replace(SortTuple {
                tuple: core::ptr::null_mut(),
                datum1,
                isnull1: is_null,
            })?;
        }
    } else {
        datum_put_flush(st, next);
        st.puttuple_common(core::ptr::null_mut(), datum1, is_null, 0)?;
    }
    Ok(datum_put_window(st))
}

impl<'m> TuplesortData<'m> {
    /// `tuplesort_puttuple_common`; the useAbbrev arm lives in puttuple_full
    /// (tuplen==0 puts are by-value datums, never abbreviated).
    /// SortTuple fields arrive as scalars (registers), not by-ref like C's
    /// `SortTuple *tuple`: the 24-byte struct would bounce through the stack
    /// into a wide reload that defeats store-to-load forwarding.
    #[inline]
    fn puttuple_common(
        &mut self,
        tuple: *mut MinimalTupleData,
        datum1: Datum,
        isnull1: bool,
        tuplen: i64,
    ) -> PgResult<()> {
        if tuplen == 0 {
            let len = self.memtuples.len();
            if len < self.put_watermark as usize {
                // SAFETY: put_watermark <= capacity - 1 (recompute_put_watermark
                // invariant), so len < capacity; tuplen == 0 leaves avail_mem
                // untouched, matching C's no-USEMEM by-value datum put.
                unsafe {
                    core::ptr::write(
                        self.memtuples.as_mut_ptr().add(len),
                        SortTuple { tuple, datum1, isnull1 },
                    );
                    self.memtuples.set_len(len + 1);
                }
                return Ok(());
            }
            if self.status == TupSortStatus::Bounded {
                return self.puttuple_bounded(SortTuple { tuple, datum1, isnull1 });
            }
        }
        self.puttuple_full(tuple, datum1, isnull1, tuplen)
    }

    #[inline(never)]
    fn puttuple_full(
        &mut self,
        tuple: *mut MinimalTupleData,
        mut datum1: Datum,
        isnull1: bool,
        tuplen: i64,
    ) -> PgResult<()> {
        self.avail_mem -= tuplen;

        match self.status {
            TupSortStatus::Initial => {
                // C also counts tupleMem on TSS_BOUNDED puts, where it is
                // dead (bounded sorts never dump): skipped there.
                self.tuple_mem += tuplen;
                // Abbrev can only be armed in Initial: set_bound disarms it
                // before any put (C parity), so Bounded never pays this check.
                if self.abbrev.is_some() && !isnull1 {
                    // C: converter never sees NULLs (datum1 keeps the zeroed word).
                    datum1 = self.abbrev_datum1(datum1);
                }
                if self.memtuples.len() >= self.memtuples.capacity() - 1 {
                    self.grow_memtuples();
                    debug_assert!(self.memtuples.len() < self.memtuples.capacity());
                }
                let len = self.memtuples.len();
                // SAFETY: len < capacity (grow above keeps one free slot, as
                // C's memtupsize-1 check does); C's unchecked store shape.
                unsafe {
                    core::ptr::write(
                        self.memtuples.as_mut_ptr().add(len),
                        SortTuple { tuple, datum1, isnull1 },
                    );
                    self.memtuples.set_len(len + 1);
                }

                if self.bounded
                    && (self.memtuples.len() > self.bound as usize * 2
                        || (self.memtuples.len() > self.bound as usize && self.lackmem()))
                {
                    self.make_bounded_heap()?;
                    self.recompute_put_watermark();
                    return Ok(());
                }

                if self.memtuples.len() < self.memtuples.capacity() && !self.lackmem() {
                    self.recompute_put_watermark();
                    return Ok(());
                }
                self.spill_to_tape()
            }
            TupSortStatus::Bounded => {
                debug_assert!(self.abbrev.is_none());
                self.puttuple_bounded(SortTuple { tuple, datum1, isnull1 })
            }
            TupSortStatus::BuildRuns => self.puttuple_buildruns(tuple, datum1, isnull1, tuplen),
            TupSortStatus::SortedInMem | TupSortStatus::SortedOnTape | TupSortStatus::FinalMerge => {
                Err(invalid_state("tuplesort_puttuple_common"))
            }
        }
    }

    #[cold]
    #[inline(never)]
    fn spill_to_tape(&mut self) -> PgResult<()> {
        self.inittapes()?;
        self.dumptuples(false)
    }

    #[inline(never)]
    fn puttuple_buildruns(
        &mut self,
        tuple: *mut MinimalTupleData,
        mut datum1: Datum,
        isnull1: bool,
        tuplen: i64,
    ) -> PgResult<()> {
        self.tuple_mem += tuplen;
        if self.abbrev.is_some() && !isnull1 {
            datum1 = self.abbrev_datum1(datum1);
        }
        debug_assert!(self.memtuples.len() < self.memtuples.capacity());
        self.memtuples.push(SortTuple { tuple, datum1, isnull1 });
        self.dumptuples(false)
    }

    /// `tuplesort_puttuple_common` useAbbrev arm: convert unless
    /// `consider_abort_common` fires, in which case datum1 representation is
    /// restored across every already-stored memtuple (REMOVEABBREV).
    /// Out of line: inlined it doubled puttuple_full on no-abbrev lanes
    /// (m3 sort_limit +22 instr/row, jobs -1783120589/-1783120595).
    #[inline(never)]
    /// Out of line as C keeps it (ssup->abbrev_converter is an indirect
    /// call): letting the converter fuse into the put complex regressed
    /// text_sort by register pressure after the spill landing's code growth.
    #[inline(never)]
    fn abbrev_datum1(&mut self, original: Datum) -> Datum {
        if !self.consider_abort_common() {
            // SAFETY: variant putters pass live non-null datums of the armed
            // type (heap getattr / putdatum's tuplecontext copy).
            unsafe { self.abbrev.as_mut().unwrap_unchecked().convert(original) }
        } else {
            self.remove_abbrev();
            original
        }
    }

    /// `consider_abort_common`.
    fn consider_abort_common(&mut self) -> bool {
        let memtupcount = self.memtuples.len();
        if self.status == TupSortStatus::Initial && memtupcount as i64 >= self.abbrev_next {
            self.abbrev_next *= 2;
            let abbrev = self.abbrev.as_mut().expect("armed caller");
            if !abbrev.abort(memtupcount as i32) {
                return false;
            }
            let full = abbrev.full_comparator;
            self.sort_keys[0].comparator = full;
            self.abbrev = None;
            return true;
        }
        false
    }

    /// `removeabbrev_heap`/`removeabbrev_datum`: refetch original datum1 for
    /// every stored tuple (index/cluster variants never arm).
    #[cold]
    #[inline(never)]
    fn remove_abbrev(&mut self) {
        let TuplesortData { variant, memtuples, sort_keys, .. } = self;
        match variant {
            SortVariant::Heap { tup_desc } => {
                let attno = sort_keys[0].ssup_attno as i32;
                for stup in memtuples.iter_mut() {
                    // SAFETY: live minimal tuples under this descriptor.
                    stup.datum1 = unsafe {
                        minimal_getattr(stup.tuple, attno, tup_desc, &mut stup.isnull1)
                    };
                }
            }
            SortVariant::Datum { .. } => {
                for stup in memtuples.iter_mut() {
                    stup.datum1 = Datum::from_usize(stup.tuple as usize);
                }
            }
            _ => unreachable!("abbreviation armed on a non-heap/datum sort"),
        }
    }

    /// `tuplesort_updatemax`: disk usage dominates memory usage.
    fn updatemax(&mut self) {
        let (is_disk, space_used) = match &self.tapes {
            Some(ts) => (true, ts.tapeset.blocks() * tape::BLCKSZ as i64),
            None => (false, self.allowed_mem - self.avail_mem),
        };
        if (is_disk && !self.is_max_space_disk)
            || (is_disk == self.is_max_space_disk && space_used > self.max_space)
        {
            self.max_space = space_used;
            self.is_max_space_disk = is_disk;
            self.max_space_status = self.status;
        }
    }

    fn recompute_put_watermark(&mut self) {
        self.put_watermark = if self.status != TupSortStatus::Initial || self.lackmem() {
            0
        } else {
            // capacity <= i32::MAX (grow_memtuples clamp), bound >= 0.
            let cap_limit = (self.memtuples.capacity() - 1) as u32;
            if self.bounded {
                cap_limit.min(self.bound as u32)
            } else {
                cap_limit
            }
        };
    }

    /// TSS_BOUNDED arm; out of line so the TSS_INITIAL fast path stays lean
    /// (C's shape: the arm's work is behind the comparetup fn pointer).
    /// Discard leg in the body, sift leg outlined; kept out of line itself —
    /// inlining it into the put spine cost the qsort lanes 7% instr
    /// (microbench 1c6520c3: tsort_int4_* 0.84x -> 0.90x).
    #[inline(never)]
    fn puttuple_bounded(&mut self, tuple: SortTuple) -> PgResult<()> {
        // SAFETY: TSS_BOUNDED invariant — memtuples holds exactly `bound` >= 1
        // tuples from make_bounded_heap on.
        let heap_top = unsafe { *self.memtuples.get_unchecked(0) };
        // datum1 decides alone when only_key, or Datum with abbrev disarmed
        // (set_bound; tiebreak 0) — result-identical minus the per-put CmpCtx/
        // dispatch ladder (docs/optimizations/orderby.md); generic arm outlined.
        let compare = if self.only_key || matches!(self.variant, SortVariant::Datum { .. }) {
            debug_assert!(self.abbrev.is_none());
            // SAFETY: non-IndexHash variants carry >=1 key (begin_* asserts);
            // IndexHash sorts are never bounded.
            let key0 = unsafe { self.sort_keys.get_unchecked(0) };
            ssup::apply_sort_comparator_as_in(
                key0.comparator,
                self.mcx,
                tuple.datum1,
                tuple.isnull1,
                heap_top.datum1,
                heap_top.isnull1,
                key0,
            )
        } else {
            self.bounded_cmp_generic(&tuple, &heap_top)
        };
        // Rowref mode (rule 2): a full-key tie against the boundary resolves
        // by rowref — reversed domain, so the incoming tuple wins (replaces
        // the root) iff its rowref is SMALLER (physically earlier).
        let compare = if compare == 0 && self.rowref_mode {
            stup_rowref(&heap_top).cmp(&stup_rowref(&tuple)) as i32
        } else {
            compare
        };
        if compare <= 0 {
            // Tie tracking: an incoming full-key tie against the boundary is
            // discarded here — which equal-key rows survive is now
            // arrival-order dependent.
            if compare == 0 && self.tie_track {
                self.tie_dirty = true;
            }
            self.free_sort_tuple(&tuple);
            cfi()
        } else {
            self.puttuple_bounded_replace(tuple)
        }
    }

    #[inline(never)]
    fn bounded_cmp_generic(&self, a: &SortTuple, b: &SortTuple) -> i32 {
        let ctx = ctx!(self);
        dispatch_cmp!(ctx, |cmp| cmp(a, b))
    }

    #[inline(never)]
    fn puttuple_bounded_replace(&mut self, tuple: SortTuple) -> PgResult<()> {
        let top = self.memtuples[0];
        if !self.tie_track {
            self.free_sort_tuple(&top);
        }
        let mut tuples = mem::replace(&mut self.memtuples, PgVec::new_in(self.mcx));
        let count = tuples.len();
        let mut tie = false;
        let rowref_mode = self.rowref_mode;
        let result = {
            let ctx = ctx!(self);
            dispatch_cmp!(ctx, |cmp| {
                if rowref_mode {
                    // Rule 2: sift under the (key, rowref) total order so
                    // equal-key heap members keep their rowref rank.
                    heap_replace_top(rowref_cmp(cmp), &mut tuples, count, tuple)
                } else {
                    let r = heap_replace_top(cmp, &mut tuples, count, tuple);
                    if self.tie_track {
                        // Evicted boundary member vs the NEW boundary: equal =
                        // an equal-key member remains (tie selection happened);
                        // strictly improved = every earlier tie event was at a
                        // key the boundary now strictly beats — clear. The
                        // evicted tuple's free is deferred below so the compare
                        // reads live bytes (accounting order is invisible;
                        // nothing allocates in between).
                        tie = cmp(&top, &tuples[0]) == 0;
                    }
                    r
                }
            })
        };
        self.memtuples = tuples;
        if self.tie_track {
            self.tie_dirty = tie;
            self.free_sort_tuple(&top);
        }
        result
    }

    /// `grow_memtuples`; chunk space approximated as capacity * sizeof(SortTuple).
    #[inline(never)]
    fn grow_memtuples(&mut self) -> bool {
        let memtupsize = self.memtuples.capacity();
        let mem_now_used = self.allowed_mem - self.avail_mem;

        if !self.grow_memtuples {
            return false;
        }

        let newmemtupsize = if mem_now_used <= self.avail_mem {
            if memtupsize < (i32::MAX / 2) as usize {
                memtupsize * 2
            } else {
                self.grow_memtuples = false;
                i32::MAX as usize
            }
        } else {
            let grow_ratio = self.allowed_mem as f64 / mem_now_used as f64;
            let mut newsize = (memtupsize as f64 * grow_ratio) as usize;
            newsize = newsize.min(i32::MAX as usize);
            self.grow_memtuples = false;
            if newsize < memtupsize + 1 {
                newsize = memtupsize + 1;
            }
            newsize
        };

        if newmemtupsize <= memtupsize
            || self.avail_mem
                < ((newmemtupsize - memtupsize) * mem::size_of::<SortTuple>()) as i64
        {
            self.grow_memtuples = false;
            return false;
        }

        self.avail_mem += (memtupsize * mem::size_of::<SortTuple>()) as i64;
        self.memtuples.reserve_exact(newmemtupsize - self.memtuples.len());
        self.avail_mem -= (self.memtuples.capacity() * mem::size_of::<SortTuple>()) as i64;
        debug_assert!(!self.lackmem());
        true
    }

    #[inline]
    fn lackmem(&self) -> bool {
        self.avail_mem < 0
    }

    /// `free_sort_tuple`: FREEMEM accounting + the physical pfree. Bounded
    /// sorts allocate caller tuples in an aset (begin_common mirrors C's
    /// TupleSortUseBumpTupleCxt), so the evicted tuple's bytes really return
    /// here — footprint tracks the bound, not the input. The caller must not
    /// read `stup.tuple` afterwards (C nulls the pointer; ours are `Copy`
    /// locals that die at the call site).
    #[inline]
    fn free_sort_tuple(&mut self, stup: &SortTuple) {
        if stup.tuple.is_null() {
            return;
        }
        self.avail_mem += freed_space(self.free_typlen, stup);
        dealloc_stup(self.tuplecontext.mcx(), self.free_typlen, stup);
    }

    /// Wholesale release of the caller-tuples context at a batch boundary
    /// (`tuplesort_reset` -> `tuplesort_begin_batch`; dumptuples' post-run
    /// reset). C never frees the surviving tuples one by one first — readout
    /// and writetup leave them allocated and the context reclaims them in
    /// bulk (tuplesort_free resets the whole sort context; begin_batch
    /// recreates the child). The bounded-capable arm is an exact-accounting
    /// aset whose reset() leak canary would (by its own contract) trip on
    /// those still-charged bytes, so when bytes remain the context is
    /// recreated instead — C's exact per-batch lifecycle, drop releases
    /// bytes and charge together. The bump arm keeps reset()'s keeper-window
    /// reuse, and an already-empty aset takes the cheap reset path.
    fn reset_tuplecontext(&mut self) {
        if self.sortopt & TUPLESORT_ALLOWBOUNDED != 0 && self.tuplecontext.used() != 0 {
            self.tuplecontext = self.mcx.context().new_child("Caller tuples");
        } else {
            self.tuplecontext.reset();
        }
    }

    /// `tuplesort_sort_memtuples`: comparator-identity specialization dispatch.
    fn sort_memtuples(&mut self) -> PgResult<()> {
        if self.memtuples.len() <= 1 {
            return Ok(());
        }
        let mut tuples = mem::replace(&mut self.memtuples, PgVec::new_in(self.mcx));
        let result = self.sort_memtuples_inner(&mut tuples);
        self.memtuples = tuples;
        result?;
        if let Some(err) = self.unique_violation.take() {
            return Err(err);
        }
        Ok(())
    }

    fn sort_memtuples_inner(&self, tuples: &mut [SortTuple]) -> PgResult<()> {
        if self.radix_applies(tuples.len()) && self.radix_sort_abbrev(tuples)? {
            return Ok(());
        }
        let ctx = ctx!(self);
        if self.have_datum1 || matches!(self.variant, SortVariant::IndexHash { .. }) {
            if self.mksort_applies(tuples.len()) {
                return self.mksort_heap(tuples);
            }
            if matches!(self.variant, SortVariant::IndexHash { .. })
                || tuples.iter().any(|t| t.isnull1)
            {
                dispatch_cmp!(ctx, |cmp| qsort_tuple(tuples, cmp))
            } else {
                dispatch_cmp!(@via comparetup_spec_notnull, ctx, |cmp| qsort_tuple(tuples, cmp))
            }
        } else if self.only_key {
            qsort_tuple(tuples, |a, b| {
                ssup::apply_sort_comparator_in(
                    ctx.mcx, a.datum1, a.isnull1, b.datum1, b.isnull1, &ctx.keys[0],
                )
            })
        } else {
            qsort_tuple(tuples, |a, b| ctx.comparetup(a, b))
        }
    }

    fn mksort_applies(&self, n: usize) -> bool {
        #[cfg(test)]
        if testhooks::MKSORT_DISABLE.with(|c| c.get()) {
            return false;
        }
        matches!(self.variant, SortVariant::Heap { .. })
            && self.have_datum1
            && self.sort_keys.len() >= 2
            && n >= MKSORT_MIN
    }

    /// Bentley-Sedgewick multi-key sort: leading key alone first, recurse per
    /// equal-key segment. Tie invariant: a full-key-unique input has a unique
    /// sorted permutation (== pg_qsort's); any full-key duplicate ends
    /// adjacent in its last-key segment, is detected there, and the pre-sort
    /// array is restored and re-sorted on the exact pg_qsort path — C tie
    /// order holds unconditionally.
    fn mksort_heap(&self, tuples: &mut [SortTuple]) -> PgResult<()> {
        let mut scratch: PgVec<'_, SortTuple> = PgVec::new_in(self.mcx);
        scratch.extend_from_slice(tuples);
        let ctx0 = CmpCtx {
            mcx: self.mcx,
            keys: &self.sort_keys[..1],
            // begin_common's rule: abbrev-equal words need the full tiebreak.
            only_key: self.abbrev.is_none(),
            abbrev: &self.abbrev,
            variant: &self.variant,
            unique_violation: &self.unique_violation,
        };
        let tied = if tuples.iter().any(|t| t.isnull1) {
            dispatch_cmp!(ctx0, |cmp| self.mksort_level0(tuples, cmp))?
        } else {
            dispatch_cmp!(@via comparetup_spec_notnull, ctx0, |cmp| self
                .mksort_level0(tuples, cmp))?
        };
        if tied {
            tuples.copy_from_slice(&scratch);
            let ctx = ctx!(self);
            if tuples.iter().any(|t| t.isnull1) {
                dispatch_cmp!(ctx, |cmp| qsort_tuple(tuples, cmp))?;
            } else {
                dispatch_cmp!(@via comparetup_spec_notnull, ctx, |cmp| qsort_tuple(tuples, cmp))?;
            }
        }
        Ok(())
    }

    fn mksort_level0(
        &self,
        tuples: &mut [SortTuple],
        cmp: impl Fn(&SortTuple, &SortTuple) -> i32 + Copy,
    ) -> PgResult<bool> {
        qsort_tuple(tuples, cmp)?;
        let n = tuples.len();
        let mut i = 0;
        while i < n {
            let mut j = i + 1;
            while j < n && cmp(&tuples[i], &tuples[j]) == 0 {
                j += 1;
            }
            if j - i > 1 && self.mksort_tail(&mut tuples[i..j], 1)? {
                return Ok(true);
            }
            i = j;
            cfi()?;
        }
        Ok(false)
    }

    /// Sort one equal-on-keys[..k] segment by key k, recursing into its own
    /// equal runs; true = a full-key duplicate pair exists (all keys equal).
    fn mksort_tail(&self, seg: &mut [SortTuple], k: usize) -> PgResult<bool> {
        let SortVariant::Heap { tup_desc } = &self.variant else {
            unreachable!("mksort_applies gates on Heap")
        };
        let key = &self.sort_keys[k];
        let attno = key.ssup_attno as i32;
        let mcx = self.mcx;
        let cmpk = move |a: &SortTuple, b: &SortTuple| {
            let (mut n1, mut n2) = (false, false);
            // SAFETY: heap-variant SortTuples always carry a live minimal
            // tuple copied under this descriptor.
            let (d1, d2) = unsafe {
                (
                    minimal_getattr(a.tuple, attno, tup_desc, &mut n1),
                    minimal_getattr(b.tuple, attno, tup_desc, &mut n2),
                )
            };
            ssup::apply_sort_comparator_in(mcx, d1, n1, d2, n2, key)
        };
        qsort_tuple(seg, cmpk)?;
        let last = k + 1 == self.sort_keys.len();
        let n = seg.len();
        let mut i = 0;
        while i < n {
            let mut j = i + 1;
            while j < n && cmpk(&seg[i], &seg[j]) == 0 {
                j += 1;
            }
            if j - i > 1 && (last || self.mksort_tail(&mut seg[i..j], k + 1)?) {
                return Ok(true);
            }
            i = j;
            cfi()?;
        }
        Ok(false)
    }

    #[inline(never)]
    fn make_bounded_heap(&mut self) -> PgResult<()> {
        let tupcount = self.memtuples.len();
        let bound = self.bound as usize;
        debug_assert!(self.status == TupSortStatus::Initial);
        debug_assert!(self.bounded && tupcount >= bound);

        self.reversedirection();

        let mut tuples = mem::replace(&mut self.memtuples, PgVec::new_in(self.mcx));
        let free_typlen = self.free_typlen;
        let tie_track = self.tie_track;
        let rowref_mode = self.rowref_mode;
        let tmcx = self.tuplecontext.mcx();
        let mut tie_dirty = false;
        let mut freed: i64 = 0;
        let result = (|| {
            let ctx = ctx!(self);
            dispatch_cmp!(ctx, |cmp| {
                if rowref_mode {
                    // Rule 2: the whole backlog transition runs under the
                    // (key, rowref) total order (ties never track).
                    bounded_backlog(
                        rowref_cmp(cmp),
                        tmcx,
                        &mut tuples,
                        tupcount,
                        bound,
                        false,
                        free_typlen,
                        &mut tie_dirty,
                        &mut freed,
                    )
                } else {
                    bounded_backlog(
                        cmp,
                        tmcx,
                        &mut tuples,
                        tupcount,
                        bound,
                        tie_track,
                        free_typlen,
                        &mut tie_dirty,
                        &mut freed,
                    )
                }
            })
        })();
        tuples.truncate(bound);
        self.memtuples = tuples;
        self.avail_mem += freed;
        if tie_track {
            self.tie_dirty = tie_dirty;
        }
        self.status = TupSortStatus::Bounded;
        result
    }

    fn sort_bounded_heap(&mut self) -> PgResult<()> {
        let tupcount = self.memtuples.len();
        debug_assert!(self.status == TupSortStatus::Bounded);
        debug_assert!(self.bounded && tupcount == self.bound as usize);

        let mut tuples = mem::replace(&mut self.memtuples, PgVec::new_in(self.mcx));
        let rowref_mode = self.rowref_mode;
        let result = {
            let ctx = ctx!(self);
            let mut count = tupcount;
            dispatch_cmp!(ctx, |cmp| {
                if rowref_mode {
                    // Rule 2: pop under the (key, rowref) total order — the
                    // emitted ascending run is rowref-ascending within
                    // full-key ties (physical order).
                    heapsort_bounded(rowref_cmp(cmp), &mut tuples, &mut count)
                } else {
                    heapsort_bounded(cmp, &mut tuples, &mut count)
                }
            })
        };
        self.memtuples = tuples;
        self.reversedirection();
        self.status = TupSortStatus::SortedInMem;
        self.bound_used = true;
        result
    }

    fn reversedirection(&mut self) {
        for key in self.sort_keys.iter_mut() {
            key.ssup_reverse = !key.ssup_reverse;
            key.ssup_nulls_first = !key.ssup_nulls_first;
        }
    }

    /// Hot leg = TSS_SORTEDINMEM (one predicted-true status compare); the
    /// tape arms live out of line so the in-memory get keeps its pre-spill
    /// inlining and code size.
    #[inline]
    fn gettuple_common(&mut self, forward: bool) -> PgResult<Option<SortTuple>> {
        if self.status == TupSortStatus::SortedInMem {
            debug_assert!(forward || self.sortopt & TUPLESORT_RANDOMACCESS != 0);
            if forward {
                if self.current < self.memtuples.len() {
                    let stup = self.memtuples[self.current];
                    self.current += 1;
                    return Ok(Some(stup));
                }
                self.eof_reached = true;
                if self.bounded && self.current >= self.bound as usize {
                    return Err(too_many_bounded());
                }
                Ok(None)
            } else {
                if self.current == 0 {
                    return Ok(None);
                }
                if self.eof_reached {
                    self.eof_reached = false;
                } else {
                    self.current -= 1;
                    if self.current == 0 {
                        return Ok(None);
                    }
                }
                Ok(Some(self.memtuples[self.current - 1]))
            }
        } else {
            self.gettuple_tape(forward)
        }
    }

    #[inline(never)]
    fn gettuple_tape(&mut self, forward: bool) -> PgResult<Option<SortTuple>> {
        match self.status {
            TupSortStatus::SortedOnTape => self.gettuple_ontape(forward),
            TupSortStatus::FinalMerge => {
                debug_assert!(forward);
                self.gettuple_finalmerge()
            }
            _ => Err(invalid_state("tuplesort_gettuple_common")),
        }
    }
}

// free_typlen sentinel: the image is a minimal/index tuple carrying t_len.
// Datum sorts store their begin-time typlen instead (datum images have no
// header: >0 fixed, -1 varlena).
const FREE_SIZE_TLEN: i16 = i16::MIN;

/// Put-time allocation size of a live sort tuple: the minimal-tuple image's
/// t_len (heaptuple's alloc_image, extra = 0), or the datum copy's typlen /
/// varlena size (putdatum). Must stay in lockstep with those alloc sites —
/// `dealloc_stup` rebuilds the allocation layout from it.
#[inline]
fn stup_alloc_size(free_typlen: i16, stup: &SortTuple) -> usize {
    debug_assert!(!stup.tuple.is_null());
    if free_typlen == FREE_SIZE_TLEN {
        // SAFETY: live tuplecontext image.
        (unsafe { (*stup.tuple).t_len }) as usize
    } else if free_typlen > 0 {
        free_typlen as usize
    } else {
        // SAFETY: live tuplecontext varlena image.
        unsafe { ::types_tuple::varatt::varsize_any(stup.tuple.cast_const().cast::<u8>()) }
    }
}

#[inline]
fn freed_space(free_typlen: i16, stup: &SortTuple) -> i64 {
    if stup.tuple.is_null() {
        return 0;
    }
    maxalign(stup_alloc_size(free_typlen, stup)) as i64
}

/// The physical half of C's free_sort_tuple (pfree): deallocate the tuple's
/// bytes in the caller-tuples context under the put-time layout. On the
/// unbounded arm the context is a bump arena whose deallocate is a no-op by
/// design — physical reclamation exists exactly where C has it (the
/// TUPLESORT_ALLOWBOUNDED aset). After this call the tuple bytes are dead:
/// callers order any boundary-tie compare BEFORE freeing the evictee.
#[inline]
fn dealloc_stup(tmcx: Mcx<'_>, free_typlen: i16, stup: &SortTuple) {
    let size = stup_alloc_size(free_typlen, stup);
    // SAFETY: `stup.tuple` was allocated in `tmcx` with exactly this
    // size/align pair (alloc_image's MAXIMUM_ALIGNOF == putdatum's 8) and is
    // freed at most once (eviction removes it from memtuples first).
    unsafe {
        ::mcx::Allocator::deallocate(
            &tmcx,
            core::ptr::NonNull::new_unchecked(stup.tuple.cast::<u8>()),
            core::alloc::Layout::from_size_align_unchecked(size, 8),
        );
    }
}

fn heap_insert(
    cmp: impl Fn(&SortTuple, &SortTuple) -> i32 + Copy,
    heap: &mut [SortTuple],
    count: &mut usize,
    tuple: SortTuple,
) -> PgResult<()> {
    cfi()?;
    let mut j = *count;
    *count += 1;
    while j > 0 {
        let i = (j - 1) >> 1;
        if cmp(&tuple, &heap[i]) >= 0 {
            break;
        }
        heap[j] = heap[i];
        j = i;
    }
    heap[j] = tuple;
    Ok(())
}

fn heap_delete_top(
    cmp: impl Fn(&SortTuple, &SortTuple) -> i32 + Copy,
    heap: &mut [SortTuple],
    count: &mut usize,
) -> PgResult<()> {
    *count -= 1;
    if *count == 0 {
        return Ok(());
    }
    let tuple = heap[*count];
    heap_replace_top_n(cmp, heap, *count, tuple)
}

/// 48-bit physical rowref stamped in a Heap-variant minimal tuple's
/// `mt_padding` (bytes 4..10 of the image) by `puttupleslot_rowref` (rowref
/// mode, tie-ordering rule 2): `(row_group << 32) | rg-global-row`, monotone
/// in physical position.
#[inline(always)]
fn stup_rowref(t: &SortTuple) -> u64 {
    // SAFETY: rowref mode is armed only on Heap-variant sorts (tuple is a
    // live sort-owned minimal-tuple image >= the 15-byte header) whose puts
    // stamped these bytes.
    unsafe {
        let p = t.tuple.cast::<u8>();
        let row = p.add(4).cast::<u32>().read_unaligned() as u64;
        let rg = p.add(8).cast::<u16>().read_unaligned() as u64;
        (rg << 32) | row
    }
}

/// Rowref tie-break wrapper for the REVERSED-direction bounded-heap
/// comparators (tie-ordering rule 2): under the reversed key order, full-key
/// ties rank LARGER rowrefs first (worse — evicted first), so the retained
/// set is the physically-earliest members and the final ascending output is
/// rowref-ascending within ties.
#[inline(always)]
fn rowref_cmp(
    cmp: impl Fn(&SortTuple, &SortTuple) -> i32 + Copy,
) -> impl Fn(&SortTuple, &SortTuple) -> i32 + Copy {
    move |a, b| {
        let c = cmp(a, b);
        if c != 0 {
            c
        } else {
            stup_rowref(b).cmp(&stup_rowref(a)) as i32
        }
    }
}

/// `make_bounded_heap`'s backlog transition body (extracted so the rowref
/// mode can run it under the rule-2 wrapped comparator): heapify the first
/// `bound` tuples, then feed the remainder through the bounded put rules
/// (discard on `<= 0`, else replace-top), with `tie_track`'s dirty
/// maintenance exactly as `puttuple_bounded`/`puttuple_bounded_replace`.
#[allow(clippy::too_many_arguments)]
fn bounded_backlog(
    cmp: impl Fn(&SortTuple, &SortTuple) -> i32 + Copy,
    tmcx: Mcx<'_>,
    tuples: &mut [SortTuple],
    tupcount: usize,
    bound: usize,
    tie_track: bool,
    free_typlen: i16,
    tie_dirty: &mut bool,
    freed: &mut i64,
) -> PgResult<()> {
    let mut count = 0usize;
    for i in 0..tupcount {
        if count < bound {
            let stup = tuples[i];
            heap_insert(cmp, tuples, &mut count, stup)?;
        } else {
            let c = cmp(&tuples[i], &tuples[0]);
            if c <= 0 {
                // Tie tracking: same discard/evict rules as
                // puttuple_bounded / puttuple_bounded_replace
                // (this loop is the same bounded put over the
                // pre-transition backlog).
                if c == 0 && tie_track {
                    *tie_dirty = true;
                }
                let stup = tuples[i];
                if !stup.tuple.is_null() {
                    *freed += freed_space(free_typlen, &stup);
                    // free_sort_tuple's physical half: the discarded backlog
                    // slot is never revisited (i advances; the array is
                    // truncated to `bound` after the transition).
                    dealloc_stup(tmcx, free_typlen, &stup);
                }
                cfi()?;
            } else {
                let stup = tuples[i];
                let top = tuples[0];
                heap_replace_top(cmp, tuples, count, stup)?;
                if tie_track {
                    // The evicted root's bytes must stay live for this
                    // boundary-tie compare — its free is ordered below.
                    *tie_dirty = cmp(&top, &tuples[0]) == 0;
                }
                if !top.tuple.is_null() {
                    *freed += freed_space(free_typlen, &top);
                    dealloc_stup(tmcx, free_typlen, &top);
                }
            }
        }
    }
    debug_assert!(count == bound);
    Ok(())
}

/// `sort_bounded_heap`'s pop loop (extracted for the rule-2 wrapped
/// comparator): repeatedly move the reversed-order root behind the shrinking
/// heap, leaving the array in forward sorted order.
fn heapsort_bounded(
    cmp: impl Fn(&SortTuple, &SortTuple) -> i32 + Copy,
    tuples: &mut [SortTuple],
    count: &mut usize,
) -> PgResult<()> {
    while *count > 1 {
        let stup = tuples[0];
        heap_delete_top(cmp, tuples, count)?;
        tuples[*count] = stup;
    }
    Ok(())
}

/// `tuplesort_heap_replace_top` (Knuth 5.2.3H sift-up).
fn heap_replace_top(
    cmp: impl Fn(&SortTuple, &SortTuple) -> i32 + Copy,
    heap: &mut [SortTuple],
    count: usize,
    tuple: SortTuple,
) -> PgResult<()> {
    debug_assert!(count >= 1);
    heap_replace_top_n(cmp, heap, count, tuple)
}

fn heap_replace_top_n(
    cmp: impl Fn(&SortTuple, &SortTuple) -> i32 + Copy,
    heap: &mut [SortTuple],
    n: usize,
    tuple: SortTuple,
) -> PgResult<()> {
    cfi()?;
    let mut i = 0usize;
    loop {
        let mut j = 2 * i + 1;
        if j >= n {
            break;
        }
        if j + 1 < n && cmp(&heap[j], &heap[j + 1]) > 0 {
            j += 1;
        }
        if cmp(&tuple, &heap[j]) <= 0 {
            break;
        }
        heap[i] = heap[j];
        i = j;
    }
    heap[i] = tuple;
    Ok(())
}

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_state(caller: &'static str) -> Box<PgError> {
    Box::new(PgError::error(format!("invalid tuplesort state in {caller}")))
}

#[track_caller]
#[cold]
#[inline(never)]
fn too_many_bounded() -> Box<PgError> {
    Box::new(PgError::error(
        "retrieved too many tuples in a bounded sort",
    ))
}

/// # Safety
/// `p` must be a live cluster blob written by `putheaptuple`.
unsafe fn cluster_tuple_of(p: *mut MinimalTupleData) -> ::types_tuple::htup::HeapTupleData<'static> {
    let base = p.cast_const().cast::<u8>();
    let hdr = base.cast::<ClusterTupleHeader>();
    ::types_tuple::htup::HeapTupleData::from_raw_parts(
        base.add(16),
        (*hdr).t_len,
        ::types_tuple::ItemPointerData::new((*hdr).blk, (*hdr).pos),
        ::types_core::InvalidOid,
    )
}

/// # Safety
/// `p` must be a live cluster blob written by `putheaptuple` with the
/// expression-index lane armed (itup_len != 0).
unsafe fn cluster_itup_of(p: *mut MinimalTupleData) -> nbtree::itup::ITup {
    let base = p.cast_const().cast::<u8>();
    let hdr = base.cast::<ClusterTupleHeader>();
    debug_assert!((*hdr).itup_len != 0);
    base.add(maxalign(16 + (*hdr).t_len as usize)).cast()
}
