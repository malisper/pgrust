// tuplesort.c external-sort half: inittapes/dumptuples/mergeruns (balanced
// k-way merge) + tape readtup/writetup per variant (tuplesortvariants.c).
// Parallel (worker/leader) arms are unreachable: begin_* is serial-only.
use core::mem;

use ::datum::Datum;
use ::mcx::{Mcx, PgVec};
use ::types_error::{PgError, PgResult};
use ::types_tuple::itemptr::ItemPointerData;
use ::types_tuple::MinimalTupleData;

use sort_storage::{LogicalTapeSet, TapeIdx};

#[allow(unused_imports)]
use crate::SortComparator;
use crate::{
    cfi, CmpCtx, ClusterTupleHeader, SortTuple, SortVariant, TupSortStatus, TuplesortData,
    TUPLESORT_RANDOMACCESS,
};

pub(crate) const BLCKSZ: usize = 8192;
const MINORDER: i64 = 6;
const MAXORDER: i64 = 500;
const TAPE_BUFFER_OVERHEAD: i64 = BLCKSZ as i64;
const MERGE_BUFFER_SIZE: i64 = BLCKSZ as i64 * 32;
const SLAB_SLOT_SIZE: usize = 1024;

const LEN_WORD: usize = mem::size_of::<u32>();

#[derive(Clone, Copy)]
pub(crate) struct MergeTuple {
    pub(crate) stup: SortTuple,
    srctape: i32,
}

pub(crate) struct TapeState<'m> {
    pub(crate) tapeset: LogicalTapeSet<'m>,
    max_tapes: i64,
    current_run: i32,
    input_tapes: PgVec<'m, TapeIdx>,
    n_input_runs: i64,
    output_tapes: PgVec<'m, TapeIdx>,
    n_output_runs: i64,
    dest_tape: TapeIdx,
    pub(crate) result_tape: Option<TapeIdx>,
    tape_buffer_mem: i64,
    // Slab: one SLAB_SLOT_SIZE slot per input tape + 1; free list threaded
    // through the first word of each free slot. Oversize tuples carry an
    // 8-byte size header for deallocation (C reads the chunk header instead).
    slab_arena: PgVec<'m, u64>,
    slab_free_head: *mut u8,
    pub(crate) merge_heap: PgVec<'m, MergeTuple>,
    pub(crate) last_returned: *mut u8,
    pub(crate) markpos_block: i64,
}

/// `tuplesort_merge_order`.
pub fn tuplesort_merge_order(allowed_mem: i64) -> i64 {
    let m = allowed_mem / (2 * TAPE_BUFFER_OVERHEAD + MERGE_BUFFER_SIZE);
    m.clamp(MINORDER, MAXORDER)
}

/// `merge_read_buffer_size`.
fn merge_read_buffer_size(
    avail_mem: i64,
    n_input_tapes: i64,
    n_input_runs: i64,
    max_output_tapes: i64,
) -> i64 {
    let n_output_runs = (n_input_runs + n_input_tapes - 1) / n_input_tapes;
    let n_output_tapes = n_output_runs.min(max_output_tapes);
    ((avail_mem - TAPE_BUFFER_OVERHEAD * n_output_tapes) / n_input_tapes).max(0)
}

impl<'m> TuplesortData<'m> {
    /// `inittapes` + `inittapestate`, serial arm (`mergeruns = true`).
    pub(crate) fn inittapes(&mut self) -> PgResult<()> {
        debug_assert!(self.status == TupSortStatus::Initial);
        let max_tapes = tuplesort_merge_order(self.allowed_mem);

        let tape_space = max_tapes * TAPE_BUFFER_OVERHEAD;
        let memtuples_space = (self.memtuples.capacity() * mem::size_of::<SortTuple>()) as i64;
        if tape_space + memtuples_space < self.allowed_mem {
            self.avail_mem -= tape_space;
        }

        let mut tapeset = LogicalTapeSet::create(self.mcx, false)?;
        let dest_tape = tapeset.create_tape();
        let mut output_tapes = PgVec::new_in(self.mcx);
        output_tapes.reserve(max_tapes as usize);
        output_tapes.push(dest_tape);

        self.tapes = Some(Box::new(TapeState {
            tapeset,
            max_tapes,
            current_run: 0,
            input_tapes: PgVec::new_in(self.mcx),
            n_input_runs: 0,
            output_tapes,
            n_output_runs: 1,
            dest_tape,
            result_tape: None,
            tape_buffer_mem: 0,
            slab_arena: PgVec::new_in(self.mcx),
            slab_free_head: core::ptr::null_mut(),
            merge_heap: PgVec::new_in(self.mcx),
            last_returned: core::ptr::null_mut(),
            markpos_block: 0,
        }));
        self.status = TupSortStatus::BuildRuns;
        self.put_watermark = 0;
        Ok(())
    }

    /// `dumptuples`.
    pub(crate) fn dumptuples(&mut self, alltuples: bool) -> PgResult<()> {
        if self.memtuples.len() < self.memtuples.capacity() && !self.lackmem() && !alltuples {
            return Ok(());
        }
        debug_assert!(self.status == TupSortStatus::BuildRuns);
        {
            let ts = self.tapes.as_mut().expect("BuildRuns without tapes");
            if self.memtuples.is_empty() && ts.current_run > 0 {
                return Ok(());
            }
            if ts.current_run == i32::MAX {
                return Err(too_many_runs());
            }
            if ts.current_run > 0 {
                ts.selectnewtape();
            }
            ts.current_run += 1;
        }

        self.sort_memtuples()?;

        let mut tuples = mem::replace(&mut self.memtuples, PgVec::new_in(self.mcx));
        let result = {
            let TuplesortData { tapes, variant, sortopt, .. } = &mut *self;
            let ts = tapes.as_mut().expect("BuildRuns without tapes");
            tuples
                .iter()
                .try_for_each(|stup| writetup(&mut ts.tapeset, ts.dest_tape, variant, *sortopt, stup))
        };
        tuples.clear();
        self.memtuples = tuples;
        result?;

        self.reset_tuplecontext();
        self.avail_mem += self.tuple_mem;
        self.tuple_mem = 0;

        let ts = self.tapes.as_mut().expect("BuildRuns without tapes");
        markrunend(&mut ts.tapeset, ts.dest_tape)
    }

    /// `mergeruns`: balanced k-way merge of all initial runs.
    pub(crate) fn mergeruns(&mut self) -> PgResult<()> {
        debug_assert!(self.status == TupSortStatus::BuildRuns && self.memtuples.is_empty());

        // Serialized tuples lack abbreviated keys: disable abbreviation and
        // restore the authoritative comparator for merge comparisons.
        if let Some(abbrev) = self.abbrev.take() {
            self.sort_keys[0].comparator = abbrev.full_comparator;
        }

        self.reset_tuplecontext();

        self.avail_mem += (self.memtuples.capacity() * mem::size_of::<SortTuple>()) as i64;
        self.memtuples = PgVec::new_in(self.mcx);

        let has_tuples = variant_has_tuples(&self.variant);
        let ts = self.tapes.as_mut().expect("mergeruns without tapes");
        let n_output_tapes = ts.output_tapes.len();

        let slab_slots = if has_tuples { n_output_tapes + 1 } else { 0 };
        ts.init_slab(slab_slots);
        self.avail_mem -= (slab_slots * SLAB_SLOT_SIZE) as i64;

        ts.merge_heap.reserve(n_output_tapes);
        self.avail_mem -= (n_output_tapes * mem::size_of::<MergeTuple>()) as i64;

        ts.tape_buffer_mem = self.avail_mem;
        self.avail_mem = 0;

        loop {
            let (ts, ctx, sortopt, mcx) = self.tape_cmp_parts();

            if ts.n_input_runs == 0 {
                for i in 0..ts.input_tapes.len() {
                    let t = ts.input_tapes[i];
                    ts.tapeset.close_tape(t);
                }
                ts.input_tapes = mem::replace(&mut ts.output_tapes, PgVec::new_in(mcx));
                ts.n_input_runs = ts.n_output_runs;
                ts.output_tapes.reserve(ts.input_tapes.len());
                ts.n_output_runs = 0;

                let input_buffer_size = merge_read_buffer_size(
                    ts.tape_buffer_mem,
                    ts.input_tapes.len() as i64,
                    ts.n_input_runs,
                    ts.max_tapes,
                );
                for i in 0..ts.input_tapes.len() {
                    let t = ts.input_tapes[i];
                    ts.tapeset.rewind_for_read(t, input_buffer_size as usize)?;
                }

                if (sortopt & TUPLESORT_RANDOMACCESS) == 0
                    && ts.n_input_runs <= ts.input_tapes.len() as i64
                {
                    ts.tapeset.forget_free_space();
                    dispatch_cmp!(ctx, |cmp| beginmerge(ts, &ctx, sortopt, mcx, cmp))?;
                    self.check_merge_unique()?;
                    self.status = TupSortStatus::FinalMerge;
                    return Ok(());
                }
            }

            ts.selectnewtape();
            dispatch_cmp!(ctx, |cmp| merge_one_run(ts, &ctx, sortopt, mcx, cmp))?;
            self.check_merge_unique()?;

            let ts = self.tapes.as_mut().expect("mergeruns without tapes");
            if ts.n_input_runs == 0 && ts.n_output_runs <= 1 {
                break;
            }
        }

        let ts = self.tapes.as_mut().expect("mergeruns without tapes");
        let result_tape = ts.output_tapes[0];
        ts.result_tape = Some(result_tape);
        ts.tapeset.freeze(result_tape)?;
        self.status = TupSortStatus::SortedOnTape;

        for i in 0..ts.input_tapes.len() {
            let t = ts.input_tapes[i];
            ts.tapeset.close_tape(t);
        }
        ts.input_tapes.clear();
        Ok(())
    }

    fn check_merge_unique(&mut self) -> PgResult<()> {
        if let Some(err) = self.unique_violation.take() {
            return Err(err);
        }
        Ok(())
    }

    /// `tuplesort_gettuple_common`, TSS_SORTEDONTAPE arm.
    pub(crate) fn gettuple_ontape(&mut self, forward: bool) -> PgResult<Option<SortTuple>> {
        debug_assert!(forward || self.sortopt & TUPLESORT_RANDOMACCESS != 0);
        let TuplesortData { tapes, sort_keys, variant, mcx, sortopt, eof_reached, .. } = self;
        let ts = tapes.as_mut().expect("SortedOnTape without tapes");
        let tape = ts.result_tape.expect("SortedOnTape without result tape");

        if !ts.last_returned.is_null() {
            let p = ts.last_returned;
            ts.last_returned = core::ptr::null_mut();
            ts.release_slot(*mcx, p);
        }

        if forward {
            if *eof_reached {
                return Ok(None);
            }
            let tuplen = getlen(&mut ts.tapeset, tape, true)?;
            if tuplen != 0 {
                let stup = readtup(ts, variant, *sortopt, *mcx, sort_keys, tape, tuplen)?;
                ts.last_returned = stup.tuple.cast();
                return Ok(Some(stup));
            }
            *eof_reached = true;
            return Ok(None);
        }

        // Backward.
        if *eof_reached {
            let nmoved = ts.tapeset.backspace(tape, 2 * LEN_WORD)?;
            if nmoved == 0 {
                return Ok(None);
            }
            if nmoved != 2 * LEN_WORD {
                return Err(unexpected_tape("unexpected tape position"));
            }
            *eof_reached = false;
        } else {
            let nmoved = ts.tapeset.backspace(tape, LEN_WORD)?;
            if nmoved == 0 {
                return Ok(None);
            }
            if nmoved != LEN_WORD {
                return Err(unexpected_tape("unexpected tape position"));
            }
            let tuplen = getlen(&mut ts.tapeset, tape, false)? as usize;
            let nmoved = ts.tapeset.backspace(tape, tuplen + 2 * LEN_WORD)?;
            if nmoved == tuplen + LEN_WORD {
                // Prev tuple is the first in the file; it becomes next to
                // read forward (matches the in-memory case).
                return Ok(None);
            }
            if nmoved != tuplen + 2 * LEN_WORD {
                return Err(unexpected_tape("bogus tuple length in backward scan"));
            }
        }

        let tuplen = getlen(&mut ts.tapeset, tape, false)?;
        let nmoved = ts.tapeset.backspace(tape, tuplen as usize)?;
        if nmoved != tuplen as usize {
            return Err(unexpected_tape("bogus tuple length in backward scan"));
        }
        let stup = readtup(ts, variant, *sortopt, *mcx, sort_keys, tape, tuplen)?;
        ts.last_returned = stup.tuple.cast();
        Ok(Some(stup))
    }

    /// `tuplesort_gettuple_common`, TSS_FINALMERGE arm.
    pub(crate) fn gettuple_finalmerge(&mut self) -> PgResult<Option<SortTuple>> {
        let (ts, ctx, sortopt, mcx) = self.tape_cmp_parts();

        if !ts.last_returned.is_null() {
            let p = ts.last_returned;
            ts.last_returned = core::ptr::null_mut();
            ts.release_slot(mcx, p);
        }

        if ts.merge_heap.is_empty() {
            return Ok(None);
        }
        let mt = ts.merge_heap[0];
        let src_tape_index = mt.srctape;
        let src_tape = ts.input_tapes[src_tape_index as usize];
        ts.last_returned = mt.stup.tuple.cast();

        match mergereadnext(ts, ctx.variant, sortopt, mcx, ctx.keys, src_tape)? {
            None => {
                dispatch_cmp!(ctx, |cmp| merge_heap_delete_top(&mut ts.merge_heap, cmp))?;
                ts.n_input_runs -= 1;
                ts.tapeset.close_tape(src_tape);
            }
            Some(stup) => {
                let newtup = MergeTuple { stup, srctape: src_tape_index };
                dispatch_cmp!(ctx, |cmp| merge_heap_replace_top(&mut ts.merge_heap, newtup, cmp))?;
            }
        }
        self.check_merge_unique()?;
        Ok(Some(mt.stup))
    }

    /// Split borrow: mutable tape state + shared comparison context.
    fn tape_cmp_parts(&mut self) -> (&mut TapeState<'m>, CmpCtx<'_>, i32, Mcx<'m>) {
        let TuplesortData {
            tapes, sort_keys, only_key, abbrev, variant, unique_violation, mcx, sortopt, ..
        } = self;
        let ctx = CmpCtx {
            mcx: *mcx,
            keys: sort_keys,
            only_key: *only_key,
            abbrev,
            variant,
            unique_violation,
        };
        (tapes.as_mut().expect("tape sort state missing"), ctx, *sortopt, *mcx)
    }
}

impl<'m> TapeState<'m> {
    /// `selectnewtape`.
    fn selectnewtape(&mut self) {
        if self.output_tapes.len() < self.max_tapes as usize {
            debug_assert!(self.n_output_runs as usize == self.output_tapes.len());
            let t = self.tapeset.create_tape();
            self.output_tapes.push(t);
            self.n_output_runs += 1;
            self.dest_tape = t;
        } else {
            self.dest_tape =
                self.output_tapes[(self.n_output_runs as usize) % self.output_tapes.len()];
            self.n_output_runs += 1;
        }
    }

    /// `init_slab_allocator`.
    fn init_slab(&mut self, num_slots: usize) {
        debug_assert!(self.slab_arena.is_empty());
        if num_slots == 0 {
            return;
        }
        let words = num_slots * SLAB_SLOT_SIZE / 8;
        self.slab_arena.reserve(words);
        self.slab_arena.resize(words, 0);
        let base = self.slab_arena.as_mut_ptr().cast::<u8>();
        // SAFETY: writes at slot boundaries within the num_slots*1024 arena.
        unsafe {
            for i in 0..num_slots - 1 {
                let slot = base.add(i * SLAB_SLOT_SIZE);
                (slot.cast::<*mut u8>()).write(base.add((i + 1) * SLAB_SLOT_SIZE));
            }
            (base.add((num_slots - 1) * SLAB_SLOT_SIZE).cast::<*mut u8>())
                .write(core::ptr::null_mut());
        }
        self.slab_free_head = base;
    }

    /// `tuplesort_readtup_alloc`.
    fn readtup_alloc(&mut self, mcx: Mcx<'m>, tuplen: usize) -> PgResult<*mut u8> {
        debug_assert!(!self.slab_free_head.is_null());
        if tuplen > SLAB_SLOT_SIZE || self.slab_free_head.is_null() {
            let layout = core::alloc::Layout::from_size_align(tuplen + 8, 8)
                .expect("readtup_alloc layout");
            let p: core::ptr::NonNull<u8> = ::mcx::Allocator::allocate(&mcx, layout)
                .map_err(|_| mcx.oom(tuplen + 8))?
                .cast();
            // SAFETY: fresh tuplen+8 allocation; size header consumed by
            // release_slot.
            unsafe {
                p.as_ptr().cast::<u64>().write(tuplen as u64);
                Ok(p.as_ptr().add(8))
            }
        } else {
            let slot = self.slab_free_head;
            // SAFETY: free slots store the next-free pointer in their first
            // word (init_slab/release_slot invariant).
            unsafe {
                self.slab_free_head = slot.cast::<*mut u8>().read();
            }
            Ok(slot)
        }
    }

    /// `RELEASE_SLAB_SLOT`.
    fn release_slot(&mut self, mcx: Mcx<'m>, p: *mut u8) {
        let base = self.slab_arena.as_ptr().cast::<u8>() as usize;
        let end = base + self.slab_arena.len() * 8;
        if (p as usize) >= base && (p as usize) < end {
            // SAFETY: p is a slab slot; thread it back onto the free list.
            unsafe {
                p.cast::<*mut u8>().write(self.slab_free_head);
            }
            self.slab_free_head = p;
        } else {
            // SAFETY: oversize allocation from readtup_alloc with its size in
            // the 8 bytes below p.
            unsafe {
                let raw = p.sub(8);
                let tuplen = raw.cast::<u64>().read() as usize;
                let layout = core::alloc::Layout::from_size_align_unchecked(tuplen + 8, 8);
                ::mcx::Allocator::deallocate(&mcx, core::ptr::NonNull::new_unchecked(raw), layout);
            }
        }
    }
}

pub(crate) fn variant_has_tuples(variant: &SortVariant) -> bool {
    !matches!(variant, SortVariant::Datum { byref_typlen: 0 })
}

/// `beginmerge`.
fn beginmerge<'m>(
    ts: &mut TapeState<'m>,
    ctx: &CmpCtx<'_>,
    sortopt: i32,
    mcx: Mcx<'m>,
    cmp: impl Fn(&SortTuple, &SortTuple) -> i32 + Copy,
) -> PgResult<()> {
    debug_assert!(ts.merge_heap.is_empty());
    let active_tapes = ts.input_tapes.len().min(ts.n_input_runs as usize);
    for src_tape_index in 0..active_tapes {
        let tape = ts.input_tapes[src_tape_index];
        if let Some(stup) = mergereadnext(ts, ctx.variant, sortopt, mcx, ctx.keys, tape)? {
            let mt = MergeTuple { stup, srctape: src_tape_index as i32 };
            merge_heap_insert(&mut ts.merge_heap, mt, cmp)?;
        }
    }
    Ok(())
}

/// `mergeonerun`.
fn merge_one_run<'m>(
    ts: &mut TapeState<'m>,
    ctx: &CmpCtx<'_>,
    sortopt: i32,
    mcx: Mcx<'m>,
    cmp: impl Fn(&SortTuple, &SortTuple) -> i32 + Copy,
) -> PgResult<()> {
    beginmerge(ts, ctx, sortopt, mcx, cmp)?;

    while let Some(&mt) = ts.merge_heap.first() {
        let src_tape_index = mt.srctape;
        let src_tape = ts.input_tapes[src_tape_index as usize];
        writetup(&mut ts.tapeset, ts.dest_tape, ctx.variant, sortopt, &mt.stup)?;
        if !mt.stup.tuple.is_null() {
            ts.release_slot(mcx, mt.stup.tuple.cast());
        }
        match mergereadnext(ts, ctx.variant, sortopt, mcx, ctx.keys, src_tape)? {
            Some(stup) => {
                let newtup = MergeTuple { stup, srctape: src_tape_index };
                merge_heap_replace_top(&mut ts.merge_heap, newtup, cmp)?;
            }
            None => {
                merge_heap_delete_top(&mut ts.merge_heap, cmp)?;
                ts.n_input_runs -= 1;
            }
        }
    }

    markrunend(&mut ts.tapeset, ts.dest_tape)
}

/// `mergereadnext`.
fn mergereadnext<'m>(
    ts: &mut TapeState<'m>,
    variant: &SortVariant,
    sortopt: i32,
    mcx: Mcx<'m>,
    keys: &[crate::SortSupport],
    src_tape: TapeIdx,
) -> PgResult<Option<SortTuple>> {
    let tuplen = getlen(&mut ts.tapeset, src_tape, true)?;
    if tuplen == 0 {
        return Ok(None);
    }
    Ok(Some(readtup(ts, variant, sortopt, mcx, keys, src_tape, tuplen)?))
}

// Merge heap: C's tuplesort_heap_* over memtuples, specialized to the
// (SortTuple, srctape) pairs the merge needs.
fn merge_heap_insert(
    heap: &mut PgVec<'_, MergeTuple>,
    mt: MergeTuple,
    cmp: impl Fn(&SortTuple, &SortTuple) -> i32 + Copy,
) -> PgResult<()> {
    cfi()?;
    heap.push(mt);
    let mut j = heap.len() - 1;
    while j > 0 {
        let i = (j - 1) >> 1;
        if cmp(&mt.stup, &heap[i].stup) >= 0 {
            break;
        }
        heap[j] = heap[i];
        j = i;
    }
    heap[j] = mt;
    Ok(())
}

fn merge_heap_delete_top(
    heap: &mut PgVec<'_, MergeTuple>,
    cmp: impl Fn(&SortTuple, &SortTuple) -> i32 + Copy,
) -> PgResult<()> {
    let last = heap.len() - 1;
    let mt = heap[last];
    heap.truncate(last);
    if last == 0 {
        return Ok(());
    }
    sift_down(heap, mt, cmp)
}

fn merge_heap_replace_top(
    heap: &mut PgVec<'_, MergeTuple>,
    mt: MergeTuple,
    cmp: impl Fn(&SortTuple, &SortTuple) -> i32 + Copy,
) -> PgResult<()> {
    debug_assert!(!heap.is_empty());
    sift_down(heap, mt, cmp)
}

fn sift_down(
    heap: &mut PgVec<'_, MergeTuple>,
    mt: MergeTuple,
    cmp: impl Fn(&SortTuple, &SortTuple) -> i32 + Copy,
) -> PgResult<()> {
    cfi()?;
    let n = heap.len();
    let mut i = 0usize;
    loop {
        let mut j = 2 * i + 1;
        if j >= n {
            break;
        }
        if j + 1 < n && cmp(&heap[j].stup, &heap[j + 1].stup) > 0 {
            j += 1;
        }
        if cmp(&mt.stup, &heap[j].stup) <= 0 {
            break;
        }
        heap[i] = heap[j];
        i = j;
    }
    heap[i] = mt;
    Ok(())
}

/// `getlen`.
pub(crate) fn getlen(tapeset: &mut LogicalTapeSet<'_>, tape: TapeIdx, eof_ok: bool) -> PgResult<u32> {
    let mut buf = [0u8; LEN_WORD];
    if tapeset.read(tape, &mut buf)? != LEN_WORD {
        return Err(unexpected_tape("unexpected end of tape"));
    }
    let len = u32::from_ne_bytes(buf);
    if len == 0 && !eof_ok {
        return Err(unexpected_tape("unexpected end of data"));
    }
    Ok(len)
}

/// `markrunend`.
pub(crate) fn markrunend(tapeset: &mut LogicalTapeSet<'_>, tape: TapeIdx) -> PgResult<()> {
    tapeset.write(tape, &0u32.to_ne_bytes())
}

/// `writetup_{heap,cluster,index,datum}` (tuplesortvariants.c).
pub(crate) fn writetup(
    tapeset: &mut LogicalTapeSet<'_>,
    tape: TapeIdx,
    variant: &SortVariant,
    sortopt: i32,
    stup: &SortTuple,
) -> PgResult<()> {
    const DATA_OFF: usize = ::types_tuple::htup::MINIMAL_TUPLE_DATA_OFFSET;
    let random = sortopt & TUPLESORT_RANDOMACCESS != 0;
    match variant {
        SortVariant::Heap { .. } => {
            // SAFETY: live tuplecontext minimal-tuple image.
            let (body, bodylen) = unsafe {
                let t_len = (*stup.tuple).t_len as usize;
                (stup.tuple.cast_const().cast::<u8>().add(DATA_OFF), t_len - DATA_OFF)
            };
            let tuplen = (bodylen + LEN_WORD) as u32;
            tapeset.write(tape, &tuplen.to_ne_bytes())?;
            // SAFETY: body/bodylen bound a live image slice.
            tapeset.write(tape, unsafe { core::slice::from_raw_parts(body, bodylen) })?;
            if random {
                tapeset.write(tape, &tuplen.to_ne_bytes())?;
            }
        }
        SortVariant::Cluster { index_desc, .. } => {
            // SAFETY: cluster blob written by putheaptuple/readtup_cluster.
            let (t_len, tid, body, itup_len) = unsafe {
                let base = stup.tuple.cast_const().cast::<u8>();
                let hdr = base.cast::<ClusterTupleHeader>();
                (
                    (*hdr).t_len as usize,
                    ItemPointerData::new((*hdr).blk, (*hdr).pos),
                    base.add(16),
                    (*hdr).itup_len as usize,
                )
            };
            // Expression-index lane record: [t_len u32][heap image][itup]
            // after the tid; the plain lane keeps [heap image] only.
            debug_assert!((itup_len != 0) == index_desc.is_some());
            let extra = if index_desc.is_some() { 4 + itup_len } else { 0 };
            let tuplen = (t_len + extra + mem::size_of::<ItemPointerData>() + LEN_WORD) as u32;
            tapeset.write(tape, &tuplen.to_ne_bytes())?;
            // SAFETY: ItemPointerData is a 6-byte repr(C) POD.
            tapeset.write(tape, unsafe {
                core::slice::from_raw_parts(
                    (&tid as *const ItemPointerData).cast::<u8>(),
                    mem::size_of::<ItemPointerData>(),
                )
            })?;
            if index_desc.is_some() {
                tapeset.write(tape, &(t_len as u32).to_ne_bytes())?;
            }
            // SAFETY: t_len-byte live image at +16.
            tapeset.write(tape, unsafe { core::slice::from_raw_parts(body, t_len) })?;
            if index_desc.is_some() {
                // SAFETY: itup_len-byte live image at maxalign(16 + t_len).
                tapeset.write(tape, unsafe {
                    core::slice::from_raw_parts(
                        body.sub(16).add(crate::maxalign(16 + t_len)),
                        itup_len,
                    )
                })?;
            }
            if random {
                tapeset.write(tape, &tuplen.to_ne_bytes())?;
            }
        }
        SortVariant::Index { .. } | SortVariant::IndexHash { .. } => {
            let itup: nbtree::itup::ITup = stup.tuple.cast_const().cast();
            // SAFETY: live index-tuple image formed by putindextuplevalues.
            let size = unsafe { nbtree::itup::index_tuple_size(itup) };
            let tuplen = (size + LEN_WORD) as u32;
            tapeset.write(tape, &tuplen.to_ne_bytes())?;
            // SAFETY: size-byte live image.
            tapeset.write(tape, unsafe { core::slice::from_raw_parts(itup, size) })?;
            if random {
                tapeset.write(tape, &tuplen.to_ne_bytes())?;
            }
        }
        SortVariant::Datum { byref_typlen } => {
            let (ptr, tuplen): (*const u8, usize) = if stup.isnull1 {
                (core::ptr::null(), 0)
            } else if *byref_typlen == 0 {
                ((&stup.datum1 as *const Datum).cast(), mem::size_of::<Datum>())
            } else {
                let p = stup.tuple.cast_const().cast::<u8>();
                let size = if *byref_typlen == -1 {
                    // SAFETY: live plain varlena image (putdatum rejects toast).
                    unsafe { ::types_tuple::varatt::varsize_any(p) }
                } else {
                    *byref_typlen as usize
                };
                (p, size)
            };
            let writtenlen = (tuplen + LEN_WORD) as u32;
            tapeset.write(tape, &writtenlen.to_ne_bytes())?;
            if tuplen > 0 {
                // SAFETY: tuplen-byte live datum image (or datum1 word).
                tapeset.write(tape, unsafe { core::slice::from_raw_parts(ptr, tuplen) })?;
            }
            if random {
                tapeset.write(tape, &writtenlen.to_ne_bytes())?;
            }
        }
    }
    Ok(())
}

/// `readtup_{heap,cluster,index,datum}` (tuplesortvariants.c); tuples land in
/// slab slots (or size-headed oversize allocations).
fn readtup<'m>(
    ts: &mut TapeState<'m>,
    variant: &SortVariant,
    sortopt: i32,
    mcx: Mcx<'m>,
    keys: &[crate::SortSupport],
    tape: TapeIdx,
    len: u32,
) -> PgResult<SortTuple> {
    const DATA_OFF: usize = ::types_tuple::htup::MINIMAL_TUPLE_DATA_OFFSET;
    let random = sortopt & TUPLESORT_RANDOMACCESS != 0;
    let stup = match variant {
        SortVariant::Heap { tup_desc } => {
            let bodylen = len as usize - LEN_WORD;
            let tuplen = bodylen + DATA_OFF;
            let p = ts.readtup_alloc(mcx, tuplen)?;
            // SAFETY: fresh tuplen-byte allocation; t_len is the first field.
            unsafe {
                p.cast::<u32>().write(tuplen as u32);
                ts.tape_read_exact(tape, core::slice::from_raw_parts_mut(p.add(DATA_OFF), bodylen))?;
            }
            let tuple = p.cast::<MinimalTupleData>();
            let mut isnull1 = false;
            // SAFETY: image just formed under this descriptor.
            let datum1 = unsafe {
                crate::minimal_getattr(tuple, keys[0].ssup_attno as i32, tup_desc, &mut isnull1)
            };
            SortTuple { tuple, datum1, isnull1 }
        }
        SortVariant::Cluster { tup_desc, attnums, index_desc, .. } => {
            let mut tid = [0u8; 6];
            ts.tape_read_exact(tape, &mut tid)?;
            let (t_len, itup_len) = if index_desc.is_some() {
                let mut tl = [0u8; 4];
                ts.tape_read_exact(tape, &mut tl)?;
                let t_len = u32::from_ne_bytes(tl) as usize;
                (t_len, len as usize - mem::size_of::<ItemPointerData>() - LEN_WORD - 4 - t_len)
            } else {
                (len as usize - mem::size_of::<ItemPointerData>() - LEN_WORD, 0)
            };
            let itup_off = crate::maxalign(16 + t_len);
            let p = ts.readtup_alloc(mcx, itup_off + itup_len)?;
            // SAFETY: fresh allocation; blob layout per putheaptuple.
            let stored = unsafe {
                let tidv = tid.as_ptr().cast::<ItemPointerData>().read_unaligned();
                let hdr = p.cast::<ClusterTupleHeader>();
                (*hdr).t_len = t_len as u32;
                (*hdr).blk = ::types_tuple::ItemPointerGetBlockNumberNoCheck(&tidv);
                (*hdr).pos = tidv.ip_posid;
                (*hdr).itup_len = itup_len as u32;
                ts.tape_read_exact(tape, core::slice::from_raw_parts_mut(p.add(16), t_len))?;
                if itup_len != 0 {
                    ts.tape_read_exact(
                        tape,
                        core::slice::from_raw_parts_mut(p.add(itup_off), itup_len),
                    )?;
                }
                ::types_tuple::htup::HeapTupleData::from_raw_parts(
                    p.add(16),
                    t_len as u32,
                    tidv,
                    ::types_core::InvalidOid,
                )
            };
            let mut isnull1 = false;
            // SAFETY: images just read under their descriptors.
            let datum1 = unsafe {
                match index_desc {
                    Some(idesc) => nbtree::itup::index_getattr(
                        p.add(itup_off).cast_const().cast(),
                        1,
                        idesc,
                        &mut isnull1,
                    ),
                    None => {
                        ::types_tuple::heap_getattr(&stored, attnums[0] as i32, tup_desc, &mut isnull1)
                    }
                }
            };
            SortTuple { tuple: p.cast(), datum1, isnull1 }
        }
        SortVariant::Index { tup_desc, .. } | SortVariant::IndexHash { tup_desc, .. } => {
            let tuplen = len as usize - LEN_WORD;
            let p = ts.readtup_alloc(mcx, tuplen)?;
            // SAFETY: fresh tuplen-byte allocation.
            unsafe {
                ts.tape_read_exact(tape, core::slice::from_raw_parts_mut(p, tuplen))?;
            }
            let itup: nbtree::itup::ITup = p.cast_const().cast();
            let mut isnull1 = false;
            // SAFETY: image just read under this descriptor.
            let datum1 = unsafe { nbtree::itup::index_getattr(itup, 1, tup_desc, &mut isnull1) };
            SortTuple { tuple: p.cast(), datum1, isnull1 }
        }
        SortVariant::Datum { byref_typlen } => {
            let tuplen = len as usize - LEN_WORD;
            if tuplen == 0 {
                SortTuple { tuple: core::ptr::null_mut(), datum1: Datum::null(), isnull1: true }
            } else if *byref_typlen == 0 {
                debug_assert!(tuplen == mem::size_of::<Datum>());
                let mut buf = [0u8; 8];
                ts.tape_read_exact(tape, &mut buf)?;
                SortTuple {
                    tuple: core::ptr::null_mut(),
                    // Full 8-byte Datum word on every target (writetup wrote
                    // size_of::<Datum>() == 8; a usize round-trip would
                    // truncate on 32-bit wasm — ILP32 Datum-word audit).
                    datum1: Datum::from_u64(u64::from_ne_bytes(buf)),
                    isnull1: false,
                }
            } else {
                let p = ts.readtup_alloc(mcx, tuplen)?;
                // SAFETY: fresh tuplen-byte allocation.
                unsafe {
                    ts.tape_read_exact(tape, core::slice::from_raw_parts_mut(p, tuplen))?;
                }
                SortTuple {
                    tuple: p.cast(),
                    datum1: Datum::from_usize(p as usize),
                    isnull1: false,
                }
            }
        }
    };
    if random {
        let mut trail = [0u8; LEN_WORD];
        ts.tape_read_exact(tape, &mut trail)?;
    }
    Ok(stup)
}

impl<'m> TapeState<'m> {
    fn tape_read_exact(&mut self, tape: TapeIdx, dst: &mut [u8]) -> PgResult<()> {
        if self.tapeset.read(tape, dst)? != dst.len() {
            return Err(unexpected_tape("unexpected end of data"));
        }
        Ok(())
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn too_many_runs() -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "cannot have more than {} runs for an external sort",
            i32::MAX
        ))
        .with_sqlstate(::types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn unexpected_tape(msg: &'static str) -> Box<PgError> {
    Box::new(PgError::error(msg))
}
