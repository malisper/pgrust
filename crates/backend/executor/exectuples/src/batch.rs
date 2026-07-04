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
const SOA_BM_WORDS: usize = SOA_MAX_ROWS.div_ceil(64);

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

/// Tuples narrower than the prefix fall back to the per-row lazy path.
#[inline(always)]
pub fn soa_deform_tuple(
    soa: &mut SoaBatch<'_>,
    plan: &SoaDeformPlan<'_>,
    atts: &[CompactAttribute],
    i: u32,
    tuple: &HeapTupleData<'_>,
) {
    let ncols = plan.ncols as usize;
    debug_assert!(soa.ncols as usize == ncols && (i as usize) < SOA_MAX_ROWS);
    debug_assert!(ncols <= atts.len() && (i as usize) < soa.end_off.len());
    if (tuple.t_data().natts() as usize) < ncols {
        soa.fallback[(i / 64) as usize] |= 1u64 << (i % 64);
        return;
    }
    if tuple.has_nulls() {
        return soa_deform_tuple_nulls(soa, atts, i as usize, ncols, tuple);
    }
    let tp = tuple.getstruct();
    let idx = i as usize;
    // SAFETY: natts >= ncols and hasnulls false put every prefix column at
    // its fixed aligned offset; SoA arrays are sized ncols * SOA_MAX_ROWS.
    unsafe {
        for c in 0..ncols {
            let att = atts.get_unchecked(c);
            let off = *plan.offs.get_unchecked(c) as usize;
            *soa.values.get_unchecked_mut(c * SOA_MAX_ROWS + idx) =
                fetch_att(tp.add(off), att.attbyval, att.attlen as i32);
            *soa.isnull.get_unchecked_mut(c * SOA_MAX_ROWS + idx) = false;
        }
        *soa.end_off.get_unchecked_mut(idx) = plan.end_off;
        *soa.slow.get_unchecked_mut(idx) = false;
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
    SoaBatch<'_> { ncols, nrows, values, isnull, end_off, slow, fallback },
);
