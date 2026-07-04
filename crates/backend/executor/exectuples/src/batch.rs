// SoA page-batch deform (upstream batch executor design, CF 6176). INVARIANT:
// soa_store_prefix leaves the slot exactly as slot_getsomeattrs(ncols) would
// (resume offset + SLOW flag), so lazy deform past the prefix is unchanged.
use ::datum::Datum;
use ::mcx::{Mcx, PgVec};
use ::types_core::AttrNumber;
use ::types_slot::{SlotData, TTS_FLAG_SLOW};
use ::types_storage::bufpage::MaxHeapTuplesPerPage;
use ::types_tuple::tupmacs::{att_isnull, att_nominal_alignby, fetch_att};
use ::types_tuple::{CompactAttribute, HeapTupleData, SizeofHeapTupleHeader};

pub const SOA_MAX_ROWS: usize = MaxHeapTuplesPerPage;
pub const SOA_BM_WORDS: usize = SOA_MAX_ROWS.div_ceil(64);

/// Fixed-width prefix plan: every column below `ncols` has `attlen > 0`.
pub struct SoaDeformPlan<'mcx> {
    ncols: u16,
    end_off: u32,
    offs: PgVec<'mcx, u32>,
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
        Some(SoaDeformPlan { ncols: ncols as u16, end_off: off as u32, offs })
    }

    #[inline]
    pub fn ncols(&self) -> u16 {
        self.ncols
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
        }
    }

    #[inline]
    pub fn begin(&mut self, nrows: u32) {
        debug_assert!(nrows as usize <= SOA_MAX_ROWS);
        self.nrows = nrows;
        self.fallback = [0; SOA_BM_WORDS];
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
            return;
        }
        if tuple.has_nulls() {
            *soa.kinds.get_unchecked_mut(idx) = 1;
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
    for c in first..last {
        let att = &atts[c];
        let off = plan.offs[c] as usize;
        // SAFETY: kind-0 rows are null-free with natts >= ncols, so tp + off
        // is inside the tuple data area for every prefix column.
        unsafe {
            let values = &mut soa.values[c * SOA_MAX_ROWS..c * SOA_MAX_ROWS + n];
            let isnull = &mut soa.isnull[c * SOA_MAX_ROWS..c * SOA_MAX_ROWS + n];
            let tps = &soa.tps[..n];
            let kinds = &soa.kinds[..n];
            macro_rules! col_loop {
                (|$p:ident| $load:expr) => {
                    for i in 0..n {
                        if *kinds.get_unchecked(i) == 0 {
                            let $p: *const u8 = *tps.get_unchecked(i);
                            *values.get_unchecked_mut(i) = $load;
                            *isnull.get_unchecked_mut(i) = false;
                        }
                    }
                };
            }
            match (att.attbyval, att.attlen) {
                (true, 4) => {
                    col_loop!(|p| Datum::from_i32(p.add(off).cast::<i32>().read_unaligned()))
                }
                (true, 8) => {
                    col_loop!(|p| Datum::from_i64(p.add(off).cast::<i64>().read_unaligned()))
                }
                (true, 2) => {
                    col_loop!(|p| Datum::from_i16(p.add(off).cast::<i16>().read_unaligned()))
                }
                (true, _) => col_loop!(|p| Datum::from_char(p.add(off).cast::<i8>().read())),
                (false, _) => col_loop!(|p| Datum::from_usize(p.add(off) as usize)),
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

mcx::forget_safe_struct!(
    SoaDeformPlan<'_> { ncols, end_off, offs },
    SoaBatch<'_> { ncols, nrows, values, isnull, end_off, slow, fallback, tps, kinds },
);
