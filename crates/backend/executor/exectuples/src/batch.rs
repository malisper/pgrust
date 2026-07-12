// SoA page-batch deform (upstream batch executor design, CF 6176). INVARIANT:
// soa_store_prefix leaves the slot exactly as slot_getsomeattrs(ncols) would
// (resume offset + SLOW flag), so lazy deform past the prefix is unchanged.
use ::datum::Datum;
use ::mcx::{Mcx, PgVec};
use ::types_core::AttrNumber;
use ::types_slot::{SlotData, TTS_FLAG_SLOW};
use ::types_storage::bufpage::MaxHeapTuplesPerPage;
use ::types_tuple::tupmacs::{
    att_addlength_pointer, att_isnull, att_nominal_alignby, att_pointer_alignby, fetch_att,
};
use ::types_tuple::{CompactAttribute, HeapTupleData, SizeofHeapTupleHeader};

pub const SOA_MAX_ROWS: usize = MaxHeapTuplesPerPage;
pub const SOA_BM_WORDS: usize = SOA_MAX_ROWS.div_ceil(64);

/// Fixed-width prefix plan: every column below `ncols` has `attlen > 0`.
pub struct SoaDeformPlan<'mcx> {
    ncols: u16,
    end_off: u32,
    offs: PgVec<'mcx, u32>,
    // Deform-JIT batch kernel (docs/optimizations/jit-deform.md): replaces
    // the AOT column pass on dense full-prefix batches when armed.
    jit: Option<alloc::rc::Rc<jit_deform::DeformKernel>>,
}

impl<'mcx> SoaDeformPlan<'mcx> {
    pub fn try_new(
        mcx: Mcx<'mcx>,
        atts: &[CompactAttribute],
        ncols: usize,
    ) -> Option<SoaDeformPlan<'mcx>> {
        if ncols == 0 || ncols > atts.len() || ncols > u16::MAX as usize {
            return None;
        }
        let mut offs = ::mcx::vec_with_capacity_in_infallible(mcx, ncols);
        let mut off = 0usize;
        for att in &atts[..ncols] {
            if att.attlen <= 0 {
                return None;
            }
            off = att_nominal_alignby(off, att.attalignby);
            offs.push(off as u32);
            off += att.attlen as usize;
        }
        Some(SoaDeformPlan { ncols: ncols as u16, end_off: off as u32, offs, jit: None })
    }

    #[inline]
    pub fn ncols(&self) -> u16 {
        self.ncols
    }

    /// Arm the JIT batch kernel; layout identity with this plan is required
    /// (same ncols and offset chain) so batch output is bit-identical.
    pub fn arm_jit(&mut self, k: alloc::rc::Rc<jit_deform::DeformKernel>) {
        debug_assert!(k.ncols() == self.ncols && k.end_off() == self.end_off);
        self.jit = Some(k);
    }

    /// Placeholder for varkey-mode batches; the varkey pass never reads it.
    pub fn unused(mcx: Mcx<'mcx>) -> SoaDeformPlan<'mcx> {
        SoaDeformPlan { ncols: 0, end_off: 0, offs: PgVec::new_in(mcx), jit: None }
    }
}

pub struct SoaBatch<'mcx> {
    ncols: u16,
    nrows: u32,
    values: PgVec<'mcx, Datum>,
    isnull: PgVec<'mcx, bool>,
    end_off: PgVec<'mcx, u32>,
    slow: PgVec<'mcx, bool>,
    fallback: [u64; SOA_BM_WORDS],
    // Kind 0 rows deform column-major from tps.
    tps: PgVec<'mcx, *const u8>,
    kinds: PgVec<'mcx, u8>,
    // OR of all staged kinds: 0 = every row is kind 0 (dense lane).
    kinds_or: u8,
}

impl<'mcx> SoaBatch<'mcx> {
    pub fn new_in(mcx: Mcx<'mcx>, ncols: u16) -> SoaBatch<'mcx> {
        let cells = ncols as usize * SOA_MAX_ROWS;
        SoaBatch {
            ncols,
            nrows: 0,
            values: ::mcx::vec_from_elem_in(mcx, Datum::null(), cells),
            isnull: ::mcx::vec_from_elem_in(mcx, false, cells),
            end_off: ::mcx::vec_from_elem_in(mcx, 0u32, SOA_MAX_ROWS),
            slow: ::mcx::vec_from_elem_in(mcx, false, SOA_MAX_ROWS),
            fallback: [0; SOA_BM_WORDS],
            tps: ::mcx::vec_from_elem_in(mcx, core::ptr::null(), SOA_MAX_ROWS),
            kinds: ::mcx::vec_from_elem_in(mcx, 0u8, SOA_MAX_ROWS),
            kinds_or: 0,
        }
    }

    #[inline]
    pub fn begin(&mut self, nrows: u32) {
        debug_assert!(nrows as usize <= SOA_MAX_ROWS);
        self.nrows = nrows;
        self.fallback = [0; SOA_BM_WORDS];
        self.kinds_or = 0;
    }

    #[inline]
    pub fn ncols(&self) -> u16 {
        self.ncols
    }

    #[inline]
    pub fn nrows(&self) -> u32 {
        self.nrows
    }

    #[inline]
    pub fn is_fallback(&self, i: u32) -> bool {
        self.fallback[(i / 64) as usize] & (1u64 << (i % 64)) != 0
    }

    /// Rows the deform skipped; a batched qual must re-check these per row.
    #[inline]
    pub fn fallback_words(&self) -> &[u64] {
        &self.fallback
    }

    /// OR extra forced-fallback rows into the batch (a batched qual kernel
    /// found them undecidable — e.g. compressed/external varlena datums on
    /// the varkey lane): they take the same per-row re-check path as
    /// deform-skipped rows.
    #[inline]
    pub fn mark_fallback_words(&mut self, words: &[u64]) {
        for (w, m) in self.fallback.iter_mut().zip(words) {
            *w |= m;
        }
    }

    /// Column `c`'s values for the staged batch.
    #[inline]
    pub fn col_values(&self, c: usize) -> &[Datum] {
        &self.values[c * SOA_MAX_ROWS..c * SOA_MAX_ROWS + self.nrows as usize]
    }

    #[inline]
    pub fn col_isnull(&self, c: usize) -> &[bool] {
        &self.isnull[c * SOA_MAX_ROWS..c * SOA_MAX_ROWS + self.nrows as usize]
    }
}

/// Varlena sort-key column plan: stage per-row pointers to one `attlen == -1`
/// column (the fused-sort direct key feed; fixed-width keys use SoaDeformPlan).
#[derive(Clone, Copy)]
pub struct SoaVarKeyPlan {
    key: u16,
    // Alignment-probe start when every preceding attr is fixed-width.
    fixed_start: Option<u32>,
    key_alignby: u8,
}

impl SoaVarKeyPlan {
    pub fn try_new(atts: &[CompactAttribute], key: usize) -> Option<SoaVarKeyPlan> {
        if key >= atts.len() || key >= u16::MAX as usize || atts[key].attlen != -1 {
            return None;
        }
        if atts[..key].iter().any(|a| a.attlen == -2) {
            return None;
        }
        let mut off = 0usize;
        let mut fixed = true;
        for att in &atts[..key] {
            if att.attlen <= 0 {
                fixed = false;
                break;
            }
            off = att_nominal_alignby(off, att.attalignby);
            off += att.attlen as usize;
        }
        Some(SoaVarKeyPlan {
            key: key as u16,
            fixed_start: fixed.then_some(off as u32),
            key_alignby: atts[key].attalignby,
        })
    }
}

/// Stage row `i`'s key pointer into column 0 of `soa`; narrow tuples get the
/// fallback bit (lazy emit path). Value/null identical to slot deform of the
/// key attribute — same page pointer, same null-bitmap semantics.
#[inline(always)]
pub fn soa_stage_varkey(
    soa: &mut SoaBatch<'_>,
    plan: &SoaVarKeyPlan,
    atts: &[CompactAttribute],
    i: u32,
    tuple: &HeapTupleData<'_>,
) {
    let idx = i as usize;
    debug_assert!(soa.ncols >= 1 && idx < SOA_MAX_ROWS);
    if (tuple.t_data().natts() as usize) <= plan.key as usize {
        soa.fallback[idx / 64] |= 1u64 << (idx % 64);
        return;
    }
    if !tuple.has_nulls() {
        if let Some(start) = plan.fixed_start {
            let tp = tuple.getstruct();
            // SAFETY: null-free tuple with natts > key: the fixed prefix ends
            // at `start` and the key varlena's first byte is readable there.
            unsafe {
                let off =
                    att_pointer_alignby(start as usize, plan.key_alignby, -1, tp.add(start as usize));
                *soa.values.get_unchecked_mut(idx) = Datum::from_usize(tp.add(off) as usize);
                *soa.isnull.get_unchecked_mut(idx) = false;
            }
            return;
        }
    }
    soa_stage_varkey_walk(soa, plan, atts, idx, tuple);
}

#[inline(never)]
fn soa_stage_varkey_walk(
    soa: &mut SoaBatch<'_>,
    plan: &SoaVarKeyPlan,
    atts: &[CompactAttribute],
    idx: usize,
    tuple: &HeapTupleData<'_>,
) {
    let tp = tuple.getstruct();
    let hasnulls = tuple.has_nulls();
    // SAFETY: in-bounds offset within the image (t_len >= header).
    let bp = unsafe { tuple.header_ptr().add(SizeofHeapTupleHeader) };
    let key = plan.key as usize;
    let mut off = 0usize;
    for c in 0..=key {
        // SAFETY: c <= key < natts (checked by the caller); the walk visits
        // attributes present in the tuple, as deform_internal's slow lane.
        unsafe {
            if hasnulls && att_isnull(c, bp) {
                if c == key {
                    soa.values[idx] = Datum::null();
                    soa.isnull[idx] = true;
                    return;
                }
                continue;
            }
            let att = atts.get_unchecked(c);
            let attlen = att.attlen as i32;
            if attlen == -1 {
                off = att_pointer_alignby(off, att.attalignby, -1, tp.add(off));
            } else {
                off = att_nominal_alignby(off, att.attalignby);
            }
            if c == key {
                soa.values[idx] = Datum::from_usize(tp.add(off) as usize);
                soa.isnull[idx] = false;
                return;
            }
            off = att_addlength_pointer(off, attlen, tp.add(off));
        }
    }
}

/// Fixed-lane rows park their data pointer; hasnulls rows deform here
/// (offsets shift past nulls); narrow tuples fall back to the lazy path.
#[inline(always)]
pub fn soa_classify_row(
    soa: &mut SoaBatch<'_>,
    plan: &SoaDeformPlan<'_>,
    atts: &[CompactAttribute],
    i: u32,
    tuple: &HeapTupleData<'_>,
) {
    let ncols = plan.ncols as usize;
    let idx = i as usize;
    debug_assert!(soa.ncols as usize == ncols && idx < SOA_MAX_ROWS && ncols <= atts.len());
    // SAFETY: idx < SOA_MAX_ROWS; arrays sized SOA_MAX_ROWS.
    unsafe {
        if (tuple.t_data().natts() as usize) < ncols {
            soa.fallback[idx / 64] |= 1u64 << (idx % 64);
            *soa.kinds.get_unchecked_mut(idx) = 2;
            soa.kinds_or |= 2;
            return;
        }
        if tuple.has_nulls() {
            *soa.kinds.get_unchecked_mut(idx) = 1;
            soa.kinds_or |= 1;
            return soa_deform_tuple_nulls(soa, atts, idx, ncols, tuple);
        }
        *soa.kinds.get_unchecked_mut(idx) = 0;
        *soa.tps.get_unchecked_mut(idx) = tuple.getstruct();
        *soa.end_off.get_unchecked_mut(idx) = plan.end_off;
        *soa.slow.get_unchecked_mut(idx) = false;
    }
}

/// Column-major deform of kind-0 rows: offset/width are loop constants,
/// each inner loop a monomorphic load/store pair per row.
pub fn soa_deform_columns(
    soa: &mut SoaBatch<'_>,
    plan: &SoaDeformPlan<'_>,
    atts: &[CompactAttribute],
    qual_col_only: Option<u16>,
) {
    let n = soa.nrows as usize;
    let ncols = plan.ncols as usize;
    let (first, last) = match qual_col_only {
        Some(c) => (c as usize, c as usize + 1),
        None => (0, ncols),
    };
    // Dense lane: every staged row is kind 0, so the per-row kind test drops
    // and the isnull column becomes one vectorizable fill.
    let dense = soa.kinds_or == 0;
    if qual_col_only.is_none() {
        if let Some(k) = plan.jit.as_deref() {
            debug_assert!(k.ncols() as usize == ncols);
            // SAFETY: kind-0 rows are null-free with natts >= ncols (kernel
            // domain); the kernel stores ncols datums at SOA_MAX_ROWS*8
            // stride from &values[i] — in bounds of the ncols*SOA_MAX_ROWS
            // buffer for every i < n <= SOA_MAX_ROWS. Layout identity with
            // the plan is arm_jit's contract; output is bit-identical to the
            // interpreter pass below (jit_deform_matches_aot_and_interpreter).
            unsafe {
                let base = soa.values.as_mut_ptr();
                if dense {
                    for c in 0..ncols {
                        soa.isnull[c * SOA_MAX_ROWS..c * SOA_MAX_ROWS + n].fill(false);
                    }
                    for i in 0..n {
                        k.soa(*soa.tps.get_unchecked(i), base.add(i), SOA_MAX_ROWS * 8);
                    }
                } else {
                    // Kind-1 rows were already deformed at classify; kind-2
                    // rows carry the fallback bit — only their cells stay
                    // stale, and no reader consumes fallback cells.
                    let isnull = soa.isnull.as_mut_ptr();
                    for i in 0..n {
                        if *soa.kinds.get_unchecked(i) != 0 {
                            continue;
                        }
                        k.soa(*soa.tps.get_unchecked(i), base.add(i), SOA_MAX_ROWS * 8);
                        for c in 0..ncols {
                            *isnull.add(c * SOA_MAX_ROWS + i) = false;
                        }
                    }
                }
            }
            return;
        }
    }
    // Non-JIT pass: the generic interpreter fetch (fetch_att, runtime
    // dispatch) — the monomorphized shape-class column loops are removed
    // (docs/optimizations/jit-deform.md rung 3); JIT-unavailable environments
    // accept interpreter cost by charter.
    for c in first..last {
        let att = &atts[c];
        let off = plan.offs[c] as usize;
        let attbyval = att.attbyval;
        let attlen = att.attlen as i32;
        // SAFETY: kind-0 rows are null-free with natts >= ncols, so tp + off
        // is inside the tuple data area for every prefix column.
        unsafe {
            let values = &mut soa.values[c * SOA_MAX_ROWS..c * SOA_MAX_ROWS + n];
            let isnull = &mut soa.isnull[c * SOA_MAX_ROWS..c * SOA_MAX_ROWS + n];
            let tps = &soa.tps[..n];
            let kinds = &soa.kinds[..n];
            if dense {
                isnull.fill(false);
                for i in 0..n {
                    *values.get_unchecked_mut(i) =
                        fetch_att((*tps.get_unchecked(i)).add(off), attbyval, attlen);
                }
            } else {
                for i in 0..n {
                    if *kinds.get_unchecked(i) == 0 {
                        *values.get_unchecked_mut(i) =
                            fetch_att((*tps.get_unchecked(i)).add(off), attbyval, attlen);
                        *isnull.get_unchecked_mut(i) = false;
                    }
                }
            }
        }
    }
}

#[inline(never)]
fn soa_deform_tuple_nulls(
    soa: &mut SoaBatch<'_>,
    atts: &[CompactAttribute],
    idx: usize,
    ncols: usize,
    tuple: &HeapTupleData<'_>,
) {
    let tp = tuple.getstruct();
    // SAFETY: hasnulls tuples carry a bitmap covering natts >= ncols bits.
    let bp = unsafe { tuple.header_ptr().add(SizeofHeapTupleHeader) };
    let mut off = 0usize;
    let mut slow = false;
    for c in 0..ncols {
        // SAFETY: c < ncols <= natts; deform_internal's slow lane, fixed-width.
        unsafe {
            if att_isnull(c, bp) {
                soa.values[c * SOA_MAX_ROWS + idx] = Datum::null();
                soa.isnull[c * SOA_MAX_ROWS + idx] = true;
                slow = true;
                continue;
            }
            let att = &atts[c];
            off = att_nominal_alignby(off, att.attalignby);
            soa.values[c * SOA_MAX_ROWS + idx] =
                fetch_att(tp.add(off), att.attbyval, att.attlen as i32);
            soa.isnull[c * SOA_MAX_ROWS + idx] = false;
            off += att.attlen as usize;
        }
    }
    soa.end_off[idx] = off as u32;
    soa.slow[idx] = slow;
}

/// false = the row wasn't batch-deformed, the lazy path applies.
#[inline(always)]
pub fn soa_store_prefix<'mcx>(slot: &mut SlotData<'mcx>, soa: &SoaBatch<'_>, i: u32) -> bool {
    if soa.is_fallback(i) {
        return false;
    }
    let h = match slot {
        SlotData::BufferHeap(b) => &mut b.base,
        SlotData::Heap(h) => h,
        _ => unreachable!("soa_store_prefix on non-heap slot"),
    };
    let ncols = soa.ncols as usize;
    let idx = i as usize;
    debug_assert!(h.base.tts_values.len() >= ncols && (i as usize) < soa.end_off.len());
    // SAFETY: slot arrays span descriptor natts >= ncols (plan-build bound).
    unsafe {
        h.off = *soa.end_off.get_unchecked(idx);
        let slow = *soa.slow.get_unchecked(idx);
        let base = &mut h.base;
        for c in 0..ncols {
            *base.tts_values.get_unchecked_mut(c) = *soa.values.get_unchecked(c * SOA_MAX_ROWS + idx);
            *base.tts_isnull.get_unchecked_mut(c) = *soa.isnull.get_unchecked(c * SOA_MAX_ROWS + idx);
        }
        base.tts_nvalid = ncols as AttrNumber;
        if slow {
            base.tts_flags |= TTS_FLAG_SLOW;
        } else {
            base.tts_flags &= !TTS_FLAG_SLOW;
        }
    }
    true
}

mcx::forget_safe_nodrop!(SoaVarKeyPlan);

// jit exempt: released in exec_end_seq_scan (the bloom-filter Rc precedent).
mcx::forget_safe_struct!(
    SoaDeformPlan<'_> { ncols, end_off, offs; jit },
    SoaBatch<'_> { ncols, nrows, values, isnull, end_off, slow, fallback, tps, kinds, kinds_or },
);
