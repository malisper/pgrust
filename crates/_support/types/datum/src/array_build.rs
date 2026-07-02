use crate::datum::Datum;
use ::types_core::Oid;
use mcx::{slice_borrow_in, vec_with_capacity_in, Mcx, PgVec};
use types_error::PgResult;

pub const MAXDIM: usize = 6;
const INIT_ELEMS: usize = 64;

// C's ArrayBuildState private-subcontext model: element storage lives in the
// caller-owned child `mcx`, so teardown is that context's reset.
pub struct ArrayBuildState<'mcx> {
    pub mcx: Mcx<'mcx>,
    pub dvalues: PgVec<'mcx, Datum>,
    pub dnulls: PgVec<'mcx, bool>,
    pub nelems: i32,
    pub element_type: Oid,
    pub typlen: i16,
    pub typbyval: bool,
    pub typalign: u8,
    pub private_cxt: bool,
}

impl<'mcx> ArrayBuildState<'mcx> {
    pub fn new(mcx: Mcx<'mcx>, element_type: Oid, private_cxt: bool) -> PgResult<Self> {
        Ok(ArrayBuildState {
            mcx,
            dvalues: vec_with_capacity_in(mcx, INIT_ELEMS)?,
            dnulls: vec_with_capacity_in(mcx, INIT_ELEMS)?,
            nelems: 0,
            element_type,
            typlen: 0,
            typbyval: false,
            typalign: 0,
            private_cxt,
        })
    }

    // C: datumCopy into astate->mcontext; stable chunk addresses outlive the call.
    pub fn copy_byref(&self, bytes: &[u8]) -> PgResult<Datum> {
        let copy = slice_borrow_in(self.mcx, bytes)?;
        Ok(Datum::from_usize(copy.as_ptr() as usize))
    }
}

pub struct ArrayBuildStateArr<'mcx> {
    pub mcx: Mcx<'mcx>,
    pub data: PgVec<'mcx, u8>,
    pub nullbitmap: Option<PgVec<'mcx, u8>>,
    pub nbytes: i32,
    pub nitems: i32,
    pub ndims: i32,
    pub dims: [i32; MAXDIM],
    pub lbs: [i32; MAXDIM],
    pub array_type: Oid,
    pub element_type: Oid,
    pub private_cxt: bool,
}

// Exactly one sub-state is Some (the C scalarstate/arraystate pair).
pub struct ArrayBuildStateAny<'mcx> {
    pub scalarstate: Option<ArrayBuildState<'mcx>>,
    pub arraystate: Option<ArrayBuildStateArr<'mcx>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;

    #[test]
    fn build_state_accum_in_context() {
        let ctx = MemoryContext::new_bump("array-build-test");
        let mut st = ArrayBuildState::new(ctx.mcx(), 23, true).unwrap();
        st.dvalues.push(Datum::from_i32(7));
        st.dnulls.push(false);
        st.nelems = 1;
        let d = st.copy_byref(b"payload").unwrap();
        let p = d.as_usize() as *const u8;
        let copied = unsafe { core::slice::from_raw_parts(p, 7) };
        assert_eq!(copied, b"payload");
        assert_eq!(st.dvalues.len(), 1);
    }
}
