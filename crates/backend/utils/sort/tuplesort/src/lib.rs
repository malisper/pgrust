// tuplesort.c + tuplesortvariants.c, in-memory serial core: heap (MinimalTuple)
// and by-value datum variants, quicksort with C's comparator specializations,
// bounded top-N heapsort. External merge (tapes), parallel sort, abbreviated
// keys, and by-reference datum sorts are loud panics naming the C lane.
#![allow(non_snake_case)]

use core::mem;

use ::datum::{Datum, NullableDatum};
use ::mcx::{McxOwned, Mcx, MemoryContext, PgVec};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult};
use ::types_slot::SlotData;
use ::types_tuple::{MinimalTupleData, TupleDescData};

mod mgetattr;
mod qsort;
mod ssup;

#[cfg(test)]
mod tests;

pub use ssup::{
    apply_sort_comparator, prepare_sort_support_from_ordering_op, SortComparator, SortSupport,
    SortSupportInit,
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
    Heap { tup_desc: std::rc::Rc<TupleDescData<'static>> },
    Datum,
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
    grow_memtuples: bool,
    memtuples: PgVec<'m, SortTuple>,
    current: usize,
    eof_reached: bool,
    markpos_offset: usize,
    markpos_eof: bool,
    sort_keys: PgVec<'m, SortSupport>,
    only_key: bool,
    have_datum1: bool,
    variant: SortVariant,
}

::mcx::bind!(pub TuplesortTy => TuplesortData<'mcx>);

/// The C `Tuplesortstate *`: state + its memory contexts, one movable value.
/// Drop is `tuplesort_end`.
pub struct Tuplesort(McxOwned<TuplesortTy>);

struct CmpCtx<'a> {
    keys: &'a [SortSupport],
    only_key: bool,
    variant: &'a SortVariant,
}

impl CmpCtx<'_> {
    /// `comparetup_heap` / `comparetup_datum`: leading key from datum1.
    #[inline]
    fn comparetup(&self, a: &SortTuple, b: &SortTuple) -> i32 {
        let compare =
            apply_sort_comparator(a.datum1, a.isnull1, b.datum1, b.isnull1, &self.keys[0]);
        if compare != 0 {
            return compare;
        }
        self.comparetup_tiebreak(a, b)
    }

    /// C's specialized `qsort_tuple_{unsigned,signed,int32}_compare`: the arm
    /// constant `cmp` folds per instantiation.
    #[inline(always)]
    fn comparetup_spec(&self, cmp: SortComparator, a: &SortTuple, b: &SortTuple) -> i32 {
        let compare = ssup::apply_sort_comparator_as(
            cmp, a.datum1, a.isnull1, b.datum1, b.isnull1, &self.keys[0],
        );
        if compare != 0 {
            return compare;
        }
        if self.only_key {
            return 0;
        }
        self.comparetup_tiebreak(a, b)
    }

    /// `comparetup_heap_tiebreak` (no abbreviated-key arm: converters are
    /// never armed here); `comparetup_datum_tiebreak` reduces to 0.
    fn comparetup_tiebreak(&self, a: &SortTuple, b: &SortTuple) -> i32 {
        let SortVariant::Heap { tup_desc } = self.variant else {
            return 0;
        };
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
}

macro_rules! ctx {
    ($st:expr) => {
        CmpCtx {
            keys: &$st.sort_keys,
            only_key: $st.only_key,
            variant: &$st.variant,
        }
    };
}

impl Tuplesort {
    /// `tuplesort_begin_heap`; sort keys resolved from ordering operators via
    /// the catalog (`PrepareSortSupportFromOrderingOp`).
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

    /// `tuplesort_begin_datum`. Pass-by-reference datum sorts (the datumCopy
    /// + abbreviation lane) are a loud panic.
    pub fn begin_datum(
        datum_type: Oid,
        sort_operator: Oid,
        sort_collation: Oid,
        nulls_first_flag: bool,
        work_mem: i32,
        sortopt: i32,
    ) -> PgResult<Tuplesort> {
        let (_typlen, typbyval) = lsyscache::get_typlenbyval(datum_type)?;
        if !typbyval {
            panic!(
                "tuplesort_begin_datum: by-reference datum sort (datumCopy lane, \
                 tuplesortvariants.c) not ported for type {datum_type}"
            );
        }
        let init = SortSupportInit {
            ssup_collation: sort_collation,
            ssup_nulls_first: nulls_first_flag,
            ssup_attno: 1,
        };
        let key = prepare_sort_support_from_ordering_op(sort_operator, &init)?;
        Ok(Self::begin_datum_with_key(key, work_mem, sortopt))
    }

    /// C divergence: as [`Tuplesort::begin_heap_with_keys`], datum variant.
    pub fn begin_datum_with_key(key: SortSupport, work_mem: i32, sortopt: i32) -> Tuplesort {
        Self::begin_common(work_mem, sortopt, &[key], true, SortVariant::Datum)
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
                grow_memtuples: true,
                memtuples,
                current: 0,
                eof_reached: false,
                markpos_offset: 0,
                markpos_eof: false,
                sort_keys,
                only_key,
                have_datum1: true,
                variant,
            })
        })
        .expect("TupleSort main context construction is infallible");
        Tuplesort(owned)
    }

    /// `tuplesort_set_bound`.
    pub fn set_bound(&mut self, bound: i64) {
        self.0.with_mut(|st| {
            debug_assert!(st.status == TupSortStatus::Initial && st.memtuples.is_empty());
            debug_assert!(st.sortopt & TUPLESORT_ALLOWBOUNDED != 0);
            debug_assert!(!st.bounded);
            if bound > i64::from(i32::MAX / 2) {
                return;
            }
            st.bounded = true;
            st.bound = bound as i32;
            // C also disarms the abbreviated-key machinery; never armed here.
        })
    }

    /// `tuplesort_used_bound`.
    pub fn used_bound(&self) -> bool {
        self.0.with(|st| st.bound_used)
    }

    /// `tuplesort_puttupleslot`.
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
            let stup = SortTuple { tuple, datum1, isnull1 };
            st.puttuple_common(stup, maxalign(t_len) as i64)
        })
    }

    /// `tuplesort_putdatum`, by-value arm (`base->tuples == false`).
    pub fn putdatum(&mut self, val: Datum, is_null: bool) -> PgResult<()> {
        self.0.with_mut(|st| {
            debug_assert!(matches!(st.variant, SortVariant::Datum));
            let stup = SortTuple {
                tuple: core::ptr::null_mut(),
                datum1: if is_null { Datum::null() } else { val },
                isnull1: is_null,
            };
            st.puttuple_common(stup, 0)
        })
    }

    /// `tuplesort_performsort`.
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
            st.current = 0;
            st.eof_reached = false;
            st.markpos_offset = 0;
            st.markpos_eof = false;
            Ok(())
        })
    }

    /// `tuplesort_gettupleslot`; `abbrev` out-param elided (abbreviated keys
    /// are never armed here).
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
                // SAFETY: lifetime laundered to the slot's, as C stores the
                // borrowed pointer with shouldFree=false: contents are valid
                // only until the tuplesort is reset/ended (caller contract;
                // nodeSort clears the slot before dropping the sort).
                let mtref: &'q MinimalTupleData = unsafe { &*stup.tuple };
                exectuples::exec_store_minimal_tuple(slot, slot_mcx, mtref);
            }
            Ok(true)
        })
    }

    /// `tuplesort_getdatum`, by-value arm (no datumCopy: `copy` elided).
    pub fn getdatum(&mut self, forward: bool) -> PgResult<Option<NullableDatum>> {
        self.0.with_mut(|st| {
            debug_assert!(matches!(st.variant, SortVariant::Datum));
            Ok(st.gettuple_common(forward)?.map(|stup| NullableDatum {
                value: stup.datum1,
                isnull: stup.isnull1,
            }))
        })
    }

    /// `tuplesort_rescan` (TUPLESORT_RANDOMACCESS only).
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

    /// `tuplesort_markpos`.
    pub fn markpos(&mut self) {
        self.0.with_mut(|st| {
            debug_assert!(st.sortopt & TUPLESORT_RANDOMACCESS != 0);
            debug_assert!(st.status == TupSortStatus::SortedInMem);
            st.markpos_offset = st.current;
            st.markpos_eof = st.eof_reached;
        })
    }

    /// `tuplesort_restorepos`.
    pub fn restorepos(&mut self) {
        self.0.with_mut(|st| {
            debug_assert!(st.sortopt & TUPLESORT_RANDOMACCESS != 0);
            debug_assert!(st.status == TupSortStatus::SortedInMem);
            st.current = st.markpos_offset;
            st.eof_reached = st.markpos_eof;
        })
    }

    /// `tuplesort_end`: contexts (and every tuple) release on drop.
    pub fn end(self) {}
}

impl<'m> TuplesortData<'m> {
    /// `tuplesort_puttuple_common`. `useAbbrev` is structurally false here
    /// (no comparator carries an abbreviation converter).
    fn puttuple_common(&mut self, tuple: SortTuple, tuplen: i64) -> PgResult<()> {
        self.avail_mem -= tuplen;

        match self.status {
            TupSortStatus::Initial => {
                if self.memtuples.len() >= self.memtuples.capacity() - 1 {
                    self.grow_memtuples();
                    debug_assert!(self.memtuples.len() < self.memtuples.capacity());
                }
                self.memtuples.push(tuple);

                if self.bounded
                    && (self.memtuples.len() > self.bound as usize * 2
                        || (self.memtuples.len() > self.bound as usize && self.lackmem()))
                {
                    self.make_bounded_heap()?;
                    return Ok(());
                }

                if self.memtuples.len() < self.memtuples.capacity() && !self.lackmem() {
                    return Ok(());
                }
                panic!(
                    "tuplesort: workMem exceeded; external sort \
                     (inittapes/dumptuples, tuplesort.c) not ported"
                );
            }
            TupSortStatus::Bounded => {
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
            TupSortStatus::SortedInMem => Err(invalid_state("tuplesort_puttuple_common")),
        }
    }

    /// `grow_memtuples`; chunk-space accounting approximates C's
    /// GetMemoryChunkSpace as capacity * sizeof(SortTuple).
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
            // C's minimum-one-slot growth guarantee.
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

    /// `tuplesort_sort_memtuples`: comparator-identity dispatch to the
    /// monomorphized specializations, then onlyKey / general qsort.
    fn sort_memtuples(&mut self) -> PgResult<()> {
        if self.memtuples.len() <= 1 {
            return Ok(());
        }
        let mut tuples = mem::replace(&mut self.memtuples, PgVec::new_in(self.mcx));
        let result = {
            let ctx = ctx!(self);
            if self.have_datum1 {
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
                }
            } else if self.only_key {
                let key = &self.sort_keys[0];
                qsort_tuple(&mut tuples, |a, b| {
                    apply_sort_comparator(a.datum1, a.isnull1, b.datum1, b.isnull1, key)
                })
            } else {
                qsort_tuple(&mut tuples, |a, b| ctx.comparetup(a, b))
            }
        };
        self.memtuples = tuples;
        result
    }

    /// `make_bounded_heap`.
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

    /// `sort_bounded_heap`.
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

    /// `reversedirection`.
    fn reversedirection(&mut self) {
        for key in self.sort_keys.iter_mut() {
            key.ssup_reverse = !key.ssup_reverse;
            key.ssup_nulls_first = !key.ssup_nulls_first;
        }
    }

    /// `tuplesort_gettuple_common`, TSS_SORTEDINMEM arms.
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

/// `tuplesort_heap_insert`.
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

/// `tuplesort_heap_delete_top`.
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
