// tuplesort.c + tuplesortvariants.c in-memory serial core; external merge,
// parallel sort, abbreviated keys, byref datum sorts = loud panics naming C.
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

mod mgetattr;
mod qsort;
mod ssup;

#[cfg(test)]
mod tests;

pub use ssup::{
    apply_sort_comparator, comparator_for_index_col, comparator_for_opfamily,
    prepare_sort_support_from_ordering_op, SortComparator, SortSupport, SortSupportInit,
};

use mgetattr::minimal_getattr;
use qsort::qsort_tuple;

pub fn init_seams() {}

pub const TUPLESORT_NONE: i32 = 0;
pub const TUPLESORT_RANDOMACCESS: i32 = 1 << 0;
pub const TUPLESORT_ALLOWBOUNDED: i32 = 1 << 1;

const INITIAL_MEMTUPSIZE: usize = 1024;

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
    // byref_typlen: 0 = by-value datums (tuple stays NULL); -1/positive =
    // by-ref datums copied into tuplecontext at put (C base->tuples).
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
        let compare = ssup::apply_sort_comparator_as_in(
            cmp, self.mcx, a.datum1, a.isnull1, b.datum1, b.isnull1, &self.keys[0],
        );
        if compare != 0 {
            return compare;
        }
        if self.only_key {
            return 0;
        }
        self.comparetup_tiebreak(a, b)
    }

    /// `comparetup_heap_tiebreak`, no abbrev arm; datum tiebreak reduces to 0.
    fn comparetup_tiebreak(&self, a: &SortTuple, b: &SortTuple) -> i32 {
        match self.variant {
            SortVariant::Heap { tup_desc } => {
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
            SortVariant::Datum { .. } => 0,
            SortVariant::Index { .. } => self.comparetup_index_btree_tiebreak(a, b),
            SortVariant::IndexHash { .. } => unreachable!("comparetup dispatches IndexHash whole"),
        }
    }

    /// `comparetup_index_btree_tiebreak`, no abbrev arm. C divergence: the
    /// unique violation is deferred to performsort (no mid-qsort ereport).
    fn comparetup_index_btree_tiebreak(&self, a: &SortTuple, b: &SortTuple) -> i32 {
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
            variant: &$st.variant,
            unique_violation: &$st.unique_violation,
        }
    };
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
        for i in 0..nkeys {
            debug_assert!(att_nums[i] != 0 && sort_operators[i] != 0);
            let init = SortSupportInit {
                ssup_collation: sort_collations[i],
                ssup_nulls_first: nulls_first_flags[i],
                ssup_attno: att_nums[i],
            };
            keys.push(prepare_sort_support_from_ordering_op(sort_operators[i], &init)?);
        }
        Ok(Self::begin_heap_with_keys(tup_desc, &keys, work_mem, sortopt))
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
        Self::begin_common(work_mem, sortopt, keys, only_key, SortVariant::Heap { tup_desc })
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
            SortVariant::IndexHash { tup_desc, high_mask, low_mask, max_buckets },
        )
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
            SortVariant::Index {
                tup_desc,
                nkeys,
                enforce_unique,
                unique_nulls_not_distinct,
                index_name: std::rc::Rc::from(index_name),
            },
        )
    }

    /// `tuplesort_begin_datum`; by-reference types are a loud panic.
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
        let key = prepare_sort_support_from_ordering_op(sort_operator, &init)?;
        let byref_typlen = if typbyval { 0 } else { typlen };
        Ok(Self::begin_common(
            work_mem,
            sortopt,
            &[key],
            true,
            SortVariant::Datum { byref_typlen },
        ))
    }

    /// C divergence: as [`Tuplesort::begin_heap_with_keys`], datum variant.
    pub fn begin_datum_with_key(key: SortSupport, work_mem: i32, sortopt: i32) -> Tuplesort {
        Self::begin_common(work_mem, sortopt, &[key], true, SortVariant::Datum { byref_typlen: 0 })
    }

    fn begin_common(
        work_mem: i32,
        sortopt: i32,
        keys: &[SortSupport],
        only_key: bool,
        variant: SortVariant,
    ) -> Tuplesort {
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
            // C datumCopy into tuplecontext; the copy is the canonical value
            // (tuplesort_getdatum returns it, valid until reset/end).
            let src = val.as_usize() as *const u8;
            // SAFETY: a non-null by-ref datum is readable for its full size.
            let size = unsafe {
                if byref_typlen == -1 {
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

    /// C divergence (structural lever): batched putdatum — the per-call len
    /// memory round-trip is ~43 cyc/put on V2 (docs/benchmarks/tuplesort.md).
    #[inline]
    pub fn putdatum_batch<R>(
        &mut self,
        f: impl for<'a, 'm> FnOnce(&mut DatumPutter<'a, 'm>) -> PgResult<R>,
    ) -> PgResult<R> {
        self.0.with_mut(|st| {
            debug_assert!(matches!(st.variant, SortVariant::Datum { byref_typlen: 0 }));
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

    /// `tuplesort_gettupleslot`; `abbrev` out-param elided (never armed).
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
            Ok(st.gettuple_common(forward)?.map(|stup| NullableDatum {
                value: stup.datum1,
                isnull: stup.isnull1,
            }))
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
    datum_put_flush(st, next);
    let datum1 = if is_null { Datum::null() } else { val };
    st.puttuple_common(core::ptr::null_mut(), datum1, is_null, 0)?;
    Ok(datum_put_window(st))
}

impl<'m> TuplesortData<'m> {
    /// `tuplesort_puttuple_common`; `useAbbrev` is structurally false here.
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
        datum1: Datum,
        isnull1: bool,
        tuplen: i64,
    ) -> PgResult<()> {
        self.avail_mem -= tuplen;

        match self.status {
            TupSortStatus::Initial => {
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
                self.puttuple_bounded(SortTuple { tuple, datum1, isnull1 })
            }
            TupSortStatus::SortedInMem => Err(invalid_state("tuplesort_puttuple_common")),
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
    #[inline(never)]
    fn puttuple_bounded(&mut self, tuple: SortTuple) -> PgResult<()> {
        let compare = ctx!(self).comparetup(&tuple, &self.memtuples[0]);
        if compare <= 0 {
            self.free_sort_tuple(&tuple);
            cfi()?;
        } else {
            let top = self.memtuples[0];
            self.free_sort_tuple(&top);
            let mut tuples = mem::replace(&mut self.memtuples, PgVec::new_in(self.mcx));
            let count = tuples.len();
            heap_replace_top(&ctx!(self), &mut tuples, count, tuple)?;
            self.memtuples = tuples;
        }
        Ok(())
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
    fn free_sort_tuple(&mut self, stup: &SortTuple) {
        if !stup.tuple.is_null() {
            // SAFETY: live tuplecontext image.
            let t_len = unsafe { (*stup.tuple).t_len } as usize;
            self.avail_mem += maxalign(t_len) as i64;
        }
    }

    /// `tuplesort_sort_memtuples`: comparator-identity specialization dispatch.
    fn sort_memtuples(&mut self) -> PgResult<()> {
        if self.memtuples.len() <= 1 {
            return Ok(());
        }
        let mut tuples = mem::replace(&mut self.memtuples, PgVec::new_in(self.mcx));
        let result = {
            let ctx = ctx!(self);
            if let SortVariant::IndexHash { high_mask, low_mask, max_buckets, .. } = self.variant {
                qsort_tuple(&mut tuples, |a, b| {
                    CmpCtx::comparetup_index_hash(high_mask, low_mask, max_buckets, a, b)
                })
            } else if self.have_datum1 {
                match self.sort_keys[0].comparator {
                    SortComparator::Unsigned => qsort_tuple(&mut tuples, |a, b| {
                        ctx.comparetup_spec(SortComparator::Unsigned, a, b)
                    }),
                    SortComparator::SignedI64 => qsort_tuple(&mut tuples, |a, b| {
                        ctx.comparetup_spec(SortComparator::SignedI64, a, b)
                    }),
                    SortComparator::Int32 => qsort_tuple(&mut tuples, |a, b| {
                        ctx.comparetup_spec(SortComparator::Int32, a, b)
                    }),
                    SortComparator::Int16 => qsort_tuple(&mut tuples, |a, b| {
                        ctx.comparetup_spec(SortComparator::Int16, a, b)
                    }),
                    SortComparator::Uint32 => qsort_tuple(&mut tuples, |a, b| {
                        ctx.comparetup_spec(SortComparator::Uint32, a, b)
                    }),
                    SortComparator::TextC => qsort_tuple(&mut tuples, |a, b| {
                        ctx.comparetup_spec(SortComparator::TextC, a, b)
                    }),
                    SortComparator::Interval => qsort_tuple(&mut tuples, |a, b| {
                        ctx.comparetup_spec(SortComparator::Interval, a, b)
                    }),
                    SortComparator::BpcharC => qsort_tuple(&mut tuples, |a, b| {
                        ctx.comparetup_spec(SortComparator::BpcharC, a, b)
                    }),
                    // Shim'd comparisons are fmgr calls; nothing to fold.
                    SortComparator::Shim(_) => qsort_tuple(&mut tuples, |a, b| ctx.comparetup(a, b)),
                }
            } else if self.only_key {
                qsort_tuple(&mut tuples, |a, b| {
                    ssup::apply_sort_comparator_in(
                        ctx.mcx, a.datum1, a.isnull1, b.datum1, b.isnull1, &ctx.keys[0],
                    )
                })
            } else {
                qsort_tuple(&mut tuples, |a, b| ctx.comparetup(a, b))
            }
        };
        self.memtuples = tuples;
        result?;
        if let Some(err) = self.unique_violation.take() {
            return Err(err);
        }
        Ok(())
    }

    #[inline(never)]
    fn make_bounded_heap(&mut self) -> PgResult<()> {
        let tupcount = self.memtuples.len();
        let bound = self.bound as usize;
        debug_assert!(self.status == TupSortStatus::Initial);
        debug_assert!(self.bounded && tupcount >= bound);

        self.reversedirection();

        let mut tuples = mem::replace(&mut self.memtuples, PgVec::new_in(self.mcx));
        let mut freed: i64 = 0;
        let result = (|| {
            let ctx = ctx!(self);
            let mut count = 0usize;
            for i in 0..tupcount {
                if count < bound {
                    let stup = tuples[i];
                    heap_insert(&ctx, &mut tuples, &mut count, stup)?;
                } else if ctx.comparetup(&tuples[i], &tuples[0]) <= 0 {
                    freed += freed_space(&tuples[i]);
                    cfi()?;
                } else {
                    let stup = tuples[i];
                    freed += freed_space(&tuples[0]);
                    heap_replace_top(&ctx, &mut tuples, count, stup)?;
                }
            }
            debug_assert!(count == bound);
            Ok(())
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
            (|| {
                while count > 1 {
                    let stup = tuples[0];
                    heap_delete_top(&ctx, &mut tuples, &mut count)?;
                    tuples[count] = stup;
                }
                Ok(())
            })()
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

fn freed_space(stup: &SortTuple) -> i64 {
    if stup.tuple.is_null() {
        0
    } else {
        // SAFETY: live tuplecontext image.
        maxalign(unsafe { (*stup.tuple).t_len } as usize) as i64
    }
}

fn heap_insert(
    ctx: &CmpCtx<'_>,
    heap: &mut [SortTuple],
    count: &mut usize,
    tuple: SortTuple,
) -> PgResult<()> {
    cfi()?;
    let mut j = *count;
    *count += 1;
    while j > 0 {
        let i = (j - 1) >> 1;
        if ctx.comparetup(&tuple, &heap[i]) >= 0 {
            break;
        }
        heap[j] = heap[i];
        j = i;
    }
    heap[j] = tuple;
    Ok(())
}

fn heap_delete_top(ctx: &CmpCtx<'_>, heap: &mut [SortTuple], count: &mut usize) -> PgResult<()> {
    *count -= 1;
    if *count == 0 {
        return Ok(());
    }
    let tuple = heap[*count];
    heap_replace_top_n(ctx, heap, *count, tuple)
}

/// `tuplesort_heap_replace_top` (Knuth 5.2.3H sift-up).
fn heap_replace_top(
    ctx: &CmpCtx<'_>,
    heap: &mut [SortTuple],
    count: usize,
    tuple: SortTuple,
) -> PgResult<()> {
    debug_assert!(count >= 1);
    heap_replace_top_n(ctx, heap, count, tuple)
}

fn heap_replace_top_n(
    ctx: &CmpCtx<'_>,
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
        if j + 1 < n && ctx.comparetup(&heap[j], &heap[j + 1]) > 0 {
            j += 1;
        }
        if ctx.comparetup(&tuple, &heap[j]) <= 0 {
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
