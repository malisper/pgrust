// tuplesort.c + tuplesortvariants.c in-memory serial core; external merge,
// parallel sort, cstring datum sorts = loud panics naming C.
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
    apply_sort_comparator, apply_sort_comparator_in, comparator_for_index_col,
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

const _: () = assert!(mem::size_of::<SortTuple>() == 24);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TupSortStatus {
    Initial,
    Bounded,
    SortedInMem,
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
    },
    // tuplesort_begin_index_hash: (bucket, hash, TID) ordering off datum1.
    IndexHash {
        tup_desc: std::rc::Rc<TupleDescData<'static>>,
        high_mask: u32,
        low_mask: u32,
        max_buckets: u32,
    },
    // CLUSTER: full heap-tuple images sorted by btree index keys; attnums
    // are the heap attnos of the index key columns (expression columns are
    // rejected upstream by BuildIndexInfo).
    Cluster {
        tup_desc: std::rc::Rc<TupleDescData<'static>>,
        attnums: [i16; 32],
        nkeys: u16,
    },
}

// Blob prefix for SortVariant::Cluster images (t_self survives the sort for
// the rewrite ctid-chain mapping); image starts MAXALIGNed at +16.
#[repr(C)]
struct ClusterTupleHeader {
    t_len: u32,
    blk: u32,
    pos: u16,
    _pad: [u16; 3],
}
const _: () = assert!(mem::size_of::<ClusterTupleHeader>() == 16);


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
    variant: SortVariant,
    // Unique violation recorded mid-sort, surfaced by performsort.
    unique_violation: Cell<Option<Box<PgError>>>,
}

::mcx::bind!(pub TuplesortTy => TuplesortData<'mcx>);

/// The C `Tuplesortstate *`; Drop is `tuplesort_end`.
pub struct Tuplesort(McxOwned<TuplesortTy>);

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
                    let compare = apply_sort_comparator(datum1, isnull1, datum2, isnull2, key);
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
            SortVariant::Cluster { tup_desc, attnums, nkeys } => {
                debug_assert!(self.abbrev.is_none());
                // comparetup_cluster_tiebreak, simple-column haveDatum1 lane;
                // no TID tiebreak (C leaves equal keys in qsort order).
                let (ta, tb) = unsafe {
                    (cluster_tuple_of(a.tuple), cluster_tuple_of(b.tuple))
                };
                for nkey in 1..*nkeys as usize {
                    let key = &self.keys[nkey];
                    let (mut isnull1, mut isnull2) = (false, false);
                    // SAFETY: blobs written by putheaptuple under this descriptor.
                    let (datum1, datum2) = unsafe {
                        (
                            ::types_tuple::heap_getattr(&ta, attnums[nkey] as i32, tup_desc, &mut isnull1),
                            ::types_tuple::heap_getattr(&tb, attnums[nkey] as i32, tup_desc, &mut isnull2),
                        )
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
            self.unique_violation
                .set(Some(prev.unwrap_or_else(|| unique_violation_error(index_name))));
        }

        // SAFETY: t_tid header read of live images (contract above).
        let (tid1, tid2) = unsafe { (nbtree::itup::t_tid(tuple1), nbtree::itup::t_tid(tuple2)) };
        let compare = ::types_tuple::itemptr::ItemPointerCompare(&tid1, &tid2);
        debug_assert!(compare != 0, "ItemPointer values should never be equal");
        compare
    }
}

// BuildIndexValueDescription wiring deferred here (ported in genam): C's
// key_desc==NULL arm ("Duplicate keys exist.") is emitted instead.
#[cold]
#[inline(never)]
fn unique_violation_error(index_name: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("could not create unique index \"{index_name}\""))
            .with_sqlstate(ERRCODE_UNIQUE_VIOLATION)
            .with_detail("Duplicate keys exist.".to_string()),
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
                // Shim'd comparisons are fmgr calls; nothing to fold.
                SortComparator::Shim(_) => {
                    let $cmp = |a: &SortTuple, b: &SortTuple| __c.comparetup(a, b);
                    $body
                }
            },
        }
    }};
}

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
        // SAFETY: lifetime erasure on the relcache tupdesc; the caller keeps
        // the index relation open for the life of the sort (C's implicit
        // contract — nbtsort holds it open across the whole build).
        let tup_desc: std::rc::Rc<TupleDescData<'static>> =
            unsafe { mem::transmute(index_rel.rd_att.clone()) };
        Ok(Self::begin_index_with_keys(
            tup_desc,
            &keys,
            nkeys as u16,
            enforce_unique,
            unique_nulls_not_distinct,
            index_rel.name(),
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
        assert!(attnums[..nkeys].iter().all(|&a| a > 0), "expression index columns");
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
            SortVariant::Cluster { tup_desc: heap_tup_desc, attnums, nkeys: nkeys as u16 },
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
                tuplecontext: mcx.context().new_child_bump("Caller tuples"),
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
                max_space_status: TupSortStatus::Initial,
                sort_keys,
                only_key,
                have_datum1: true,
                abbrev,
                abbrev_next: 10,
                free_typlen,
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
                    TupSortStatus::Initial | TupSortStatus::Bounded => {
                        TuplesortMethod::StillInProgress
                    }
                },
                spaceType: TuplesortSpaceType::Memory,
                spaceUsed: (st.max_space + 1023) / 1024,
            }
        })
    }

    /// `tuplesort_reset`: recycle the batch, keep keys + memtuples capacity.
    pub fn reset(&mut self) {
        self.0.with_mut(|st| {
            st.updatemax();
            st.tuplecontext.reset();
            st.memtuples.clear();
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

    /// `tuplesort_putheaptuple` (cluster variant).
    pub fn putheaptuple(&mut self, tup: &::types_tuple::htup::HeapTupleData<'_>) -> PgResult<()> {
        self.0.with_mut(|st| {
            let SortVariant::Cluster { tup_desc, attnums, .. } = &st.variant else {
                panic!("tuplesort_putheaptuple on a non-cluster tuplesort")
            };
            let t_len = tup.t_len as usize;
            let words = (16 + t_len).div_ceil(8);
            let mut blob: PgVec<'_, u64> =
                ::mcx::vec_with_capacity_in(st.tuplecontext.mcx(), words)?;
            blob.resize(words, 0);
            let base = blob.as_mut_ptr().cast::<u8>();
            // SAFETY: fresh words*8 >= 16+t_len byte buffer; source image is
            // live for t_len bytes (HeapTupleData invariant).
            unsafe {
                let hdr = base.cast::<ClusterTupleHeader>();
                (*hdr).t_len = tup.t_len;
                (*hdr).blk = ::types_tuple::ItemPointerGetBlockNumberNoCheck(&tup.t_self);
                (*hdr).pos = tup.t_self.ip_posid;
                core::ptr::copy_nonoverlapping(tup.header_ptr(), base.add(16), t_len);
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
            // SAFETY: live image; attnums[0] is a valid user attno.
            let datum1 = unsafe {
                ::types_tuple::heap_getattr(&stored, attnums[0] as i32, tup_desc, &mut isnull1)
            };
            st.puttuple_common(
                base.cast::<MinimalTupleData>(),
                datum1,
                isnull1,
                maxalign(16 + t_len) as i64,
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
            let src = val.as_usize() as *const u8;
            // SAFETY: a non-null by-ref datum is readable for its full size.
            let size = unsafe {
                if byref_typlen == -1 {
                    // Verbatim toast-pointer copies dangle (C flattens/detoasts).
                    assert!(
                        !::types_tuple::varatt::varatt_is_1b_e(src),
                        "tuplesort_putdatum: external/expanded varlena datum (detoast lane)"
                    );
                    ::types_tuple::varatt::varsize_any(src)
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
                TupSortStatus::SortedInMem => {
                    return Err(invalid_state("tuplesort_performsort"))
                }
            }
            st.put_watermark = 0;
            st.current = 0;
            st.eof_reached = false;
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
        self.0.with_mut(|st| {
            if st.status != TupSortStatus::SortedInMem {
                return Err(invalid_state("tuplesort_skiptuples"));
            }
            if st.memtuples.len() - st.current >= ntuples as usize {
                st.current += ntuples as usize;
                return Ok(true);
            }
            st.current = st.memtuples.len();
            st.eof_reached = true;
            Ok(false)
        })
    }

    pub fn rescan(&mut self) {
        self.0.with_mut(|st| {
            debug_assert!(st.sortopt & TUPLESORT_RANDOMACCESS != 0);
            debug_assert!(st.status == TupSortStatus::SortedInMem);
            st.current = 0;
            st.eof_reached = false;
            st.markpos_offset = 0;
            st.markpos_eof = false;
        })
    }

    pub fn markpos(&mut self) {
        self.0.with_mut(|st| {
            debug_assert!(st.sortopt & TUPLESORT_RANDOMACCESS != 0);
            debug_assert!(st.status == TupSortStatus::SortedInMem);
            st.markpos_offset = st.current;
            st.markpos_eof = st.eof_reached;
        })
    }

    pub fn restorepos(&mut self) {
        self.0.with_mut(|st| {
            debug_assert!(st.sortopt & TUPLESORT_RANDOMACCESS != 0);
            debug_assert!(st.status == TupSortStatus::SortedInMem);
            st.current = st.markpos_offset;
            st.eof_reached = st.markpos_eof;
        })
    }

    pub fn end(self) {}
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
                external_sort_unported();
            }
            TupSortStatus::Bounded => {
                debug_assert!(self.abbrev.is_none());
                self.puttuple_bounded(SortTuple { tuple, datum1, isnull1 })
            }
            TupSortStatus::SortedInMem => Err(invalid_state("tuplesort_puttuple_common")),
        }
    }

    /// `tuplesort_puttuple_common` useAbbrev arm: convert unless
    /// `consider_abort_common` fires, in which case datum1 representation is
    /// restored across every already-stored memtuple (REMOVEABBREV).
    /// Out of line: inlined it doubled puttuple_full on no-abbrev lanes
    /// (m3 sort_limit +22 instr/row, jobs -1783120589/-1783120595).
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

    fn updatemax(&mut self) {
        let space_used = self.allowed_mem - self.avail_mem;
        if space_used > self.max_space {
            self.max_space = space_used;
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
        if compare <= 0 {
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
        self.free_sort_tuple(&top);
        let mut tuples = mem::replace(&mut self.memtuples, PgVec::new_in(self.mcx));
        let count = tuples.len();
        let result = {
            let ctx = ctx!(self);
            dispatch_cmp!(ctx, |cmp| heap_replace_top(cmp, &mut tuples, count, tuple))
        };
        self.memtuples = tuples;
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

    /// `free_sort_tuple`: accounting only — tuplecontext is a bump arena, so
    /// discarded bounded-sort tuples are reclaimed at end, not per-tuple as
    /// C's aset pfree does (memory-footprint divergence, not behavior).
    #[inline]
    fn free_sort_tuple(&mut self, stup: &SortTuple) {
        if stup.tuple.is_null() {
            return;
        }
        self.avail_mem += freed_space(self.free_typlen, stup);
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
        let mut freed: i64 = 0;
        let result = (|| {
            let ctx = ctx!(self);
            dispatch_cmp!(ctx, |cmp| {
                let mut count = 0usize;
                for i in 0..tupcount {
                    if count < bound {
                        let stup = tuples[i];
                        heap_insert(cmp, &mut tuples, &mut count, stup)?;
                    } else if cmp(&tuples[i], &tuples[0]) <= 0 {
                        freed += freed_space(free_typlen, &tuples[i]);
                        cfi()?;
                    } else {
                        let stup = tuples[i];
                        freed += freed_space(free_typlen, &tuples[0]);
                        heap_replace_top(cmp, &mut tuples, count, stup)?;
                    }
                }
                debug_assert!(count == bound);
                Ok(())
            })
        })();
        tuples.truncate(bound);
        self.memtuples = tuples;
        self.avail_mem += freed;
        self.status = TupSortStatus::Bounded;
        result
    }

    fn sort_bounded_heap(&mut self) -> PgResult<()> {
        let tupcount = self.memtuples.len();
        debug_assert!(self.status == TupSortStatus::Bounded);
        debug_assert!(self.bounded && tupcount == self.bound as usize);

        let mut tuples = mem::replace(&mut self.memtuples, PgVec::new_in(self.mcx));
        let result = {
            let ctx = ctx!(self);
            let mut count = tupcount;
            dispatch_cmp!(ctx, |cmp| (|| {
                while count > 1 {
                    let stup = tuples[0];
                    heap_delete_top(cmp, &mut tuples, &mut count)?;
                    tuples[count] = stup;
                }
                Ok(())
            })())
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

    fn gettuple_common(&mut self, forward: bool) -> PgResult<Option<SortTuple>> {
        match self.status {
            TupSortStatus::SortedInMem => {
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
            }
            _ => Err(invalid_state("tuplesort_gettuple_common")),
        }
    }
}

// free_typlen sentinel: the image is a minimal/index tuple carrying t_len.
// Datum sorts store their begin-time typlen instead (datum images have no
// header: >0 fixed, -1 varlena).
const FREE_SIZE_TLEN: i16 = i16::MIN;

#[inline]
fn freed_space(free_typlen: i16, stup: &SortTuple) -> i64 {
    if stup.tuple.is_null() {
        return 0;
    }
    let size = if free_typlen == FREE_SIZE_TLEN {
        // SAFETY: live tuplecontext image.
        (unsafe { (*stup.tuple).t_len }) as usize
    } else if free_typlen > 0 {
        free_typlen as usize
    } else {
        // SAFETY: live tuplecontext varlena image.
        unsafe { ::types_tuple::varatt::varsize_any(stup.tuple.cast_const().cast::<u8>()) }
    };
    maxalign(size) as i64
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

#[cold]
#[inline(never)]
fn external_sort_unported() -> ! {
    panic!(
        "tuplesort: workMem exceeded; external sort \
         (inittapes/dumptuples, tuplesort.c) not ported"
    )
}

#[cold]
#[inline(never)]
fn invalid_state(caller: &'static str) -> Box<PgError> {
    Box::new(PgError::error(format!("invalid tuplesort state in {caller}")))
}

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
