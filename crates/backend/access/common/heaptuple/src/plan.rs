use ::datum::Datum;
use ::mcx::Mcx;
use ::types_error::PgResult;
use ::types_tuple::tupmacs::{att_nominal_alignby, store_att_byval};
use ::types_tuple::{
    MinimalTupleData, TupleDescData, MAXALIGN, MINIMAL_TUPLE_OFFSET, SizeofMinimalTupleHeader,
};

use crate::tuple::MinimalTuple;

pub const FORM_PLAN_MAX_COLS: usize = 8;

#[derive(Clone, Copy, Default)]
struct FormCol {
    off: u16,
    len: u8,
}

/// Precomputed no-null layout for an all-byval fixed-width descriptor
/// (attcacheoff-class resolve-once; the layout is a function of the
/// descriptor alone when no column is NULL or varwidth).
#[derive(Clone, Copy)]
pub struct MinimalFormPlan {
    natts: u16,
    hoff: u8,
    t_hoff: u8,
    len: u32,
    cols: [FormCol; FORM_PLAN_MAX_COLS],
}

impl MinimalFormPlan {
    pub fn try_new(desc: &TupleDescData<'_>) -> Option<MinimalFormPlan> {
        let natts = desc.natts as usize;
        if natts == 0 || natts > FORM_PLAN_MAX_COLS {
            return None;
        }
        let mut cols = [FormCol::default(); FORM_PLAN_MAX_COLS];
        let mut off = 0usize;
        for (col, att) in cols[..natts].iter_mut().zip(&desc.compact_attrs[..natts]) {
            if !att.attbyval || att.attisdropped {
                return None;
            }
            if !matches!(att.attlen, 1 | 2 | 4 | 8) {
                return None;
            }
            off = att_nominal_alignby(off, att.attalignby);
            *col = FormCol { off: off as u16, len: att.attlen as u8 };
            off += att.attlen as usize;
        }
        let hoff = MAXALIGN(SizeofMinimalTupleHeader);
        Some(MinimalFormPlan {
            natts: natts as u16,
            hoff: hoff as u8,
            t_hoff: (hoff + MINIMAL_TUPLE_OFFSET) as u8,
            len: (hoff + off) as u32,
            cols,
        })
    }

    #[inline]
    pub fn natts(&self) -> usize {
        self.natts as usize
    }
}

/// [`heap_form_minimal_tuple`](crate::heap_form_minimal_tuple) with the size
/// walk and per-column layout branches hoisted into `plan`; bytes identical.
/// Caller guarantees no NULL among the first `plan.natts()` values and that
/// `plan` was built from this row's descriptor.
pub fn heap_form_minimal_tuple_planned<'mcx>(
    mcx: Mcx<'mcx>,
    plan: &MinimalFormPlan,
    values: &[Datum],
    extra: usize,
) -> PgResult<MinimalTuple<'mcx>> {
    debug_assert!(extra == MAXALIGN(extra));
    let natts = plan.natts as usize;
    let mut tuple = MinimalTuple::alloc(mcx, plan.len as usize, extra, true)?;
    // SAFETY: fresh zeroed image of plan.len bytes; plan offsets all lie in
    // [hoff, len) by construction; datums are byval per the plan gate.
    unsafe {
        let tp = tuple.tuple_mut_ptr();
        let mt = &mut *tp.cast::<MinimalTupleData>();
        mt.t_len = plan.len;
        mt.t_infomask2 = plan.natts;
        mt.t_hoff = plan.t_hoff;
        let base = tp.add(plan.hoff as usize);
        for (v, c) in values[..natts].iter().zip(&plan.cols[..natts]) {
            store_att_byval(base.add(c.off as usize), *v, c.len as i32);
        }
    }
    Ok(tuple)
}
