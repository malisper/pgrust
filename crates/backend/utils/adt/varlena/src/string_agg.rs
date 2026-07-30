//! makeStringAggState + string_agg/bytea_string_agg trans/final/combine/
//! serialize/deserialize (varlena.c). The state is C's StringInfo
//! (data/len/maxlen/cursor) hand-rolled drop-free so it can live in the
//! wholesale-reset aggcontext arena; `cursor` holds the first delimiter's
//! length, stripped only in the finalfn (parallel-agg combine contract).
//! serialize/deserialize wire format: int32 cursor, then raw data bytes
//! (varlena.c string_agg_serialize/string_agg_deserialize).

use core::alloc::Layout;

use datum::Bytea;
use mcx::{Allocator, Mcx};
use types_error::{PgError, PgResult};
use types_fmgr::FunctionCallInfoBaseData as Fcinfo;

pub struct StringAggState {
    data: *mut u8,
    len: u32,
    maxlen: u32,
    pub cursor: u32,
}

const _: () = assert!(!core::mem::needs_drop::<StringAggState>());

const INITIAL_SIZE: usize = 1024;

impl StringAggState {
    pub fn accumulated(&self) -> &[u8] {
        // SAFETY: data..data+len was written by append into a live arena
        // allocation of maxlen >= len bytes.
        unsafe { core::slice::from_raw_parts(self.data, self.len as usize) }
    }

    pub fn append(&mut self, mcx: Mcx<'_>, bytes: &[u8]) -> PgResult<()> {
        let needed = self.len as usize + bytes.len() + 1;
        if needed > self.maxlen as usize {
            let mut newlen = 2 * self.maxlen as usize;
            while needed > newlen {
                newlen *= 2;
            }
            mcx::check_alloc_size(newlen)?;
            let layout = Layout::from_size_align(newlen, 1).unwrap();
            let new = Allocator::allocate(&mcx, layout)
                .map_err(|_| mcx.oom(newlen))?
                .cast::<u8>()
                .as_ptr();
            // SAFETY: fresh allocation of newlen > len bytes; source is the
            // live previous buffer.
            unsafe { core::ptr::copy_nonoverlapping(self.data, new, self.len as usize) };
            self.data = new;
            self.maxlen = newlen as u32;
        }
        // SAFETY: maxlen - len >= bytes.len() after the growth arm above.
        unsafe {
            core::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.data.add(self.len as usize),
                bytes.len(),
            );
        }
        self.len += bytes.len() as u32;
        Ok(())
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn non_aggregate_context() -> Box<PgError> {
    Box::new(PgError::error("string_agg_transfn called in non-aggregate context"))
}

#[track_caller]
#[cold]
#[inline(never)]
fn non_aggregate_call_context() -> Box<PgError> {
    Box::new(PgError::error("aggregate function called in non-aggregate context"))
}

fn make_string_agg_state(agg_mcx: Mcx<'_>) -> PgResult<*mut StringAggState> {
    let buf_layout = Layout::from_size_align(INITIAL_SIZE, 1).unwrap();
    let data = Allocator::allocate(&agg_mcx, buf_layout)
        .map_err(|_| agg_mcx.oom(INITIAL_SIZE))?
        .cast::<u8>()
        .as_ptr();
    let layout = Layout::new::<StringAggState>();
    let raw = Allocator::allocate(&agg_mcx, layout).map_err(|_| agg_mcx.oom(layout.size()))?;
    let p = raw.cast::<StringAggState>().as_ptr();
    // SAFETY: fresh allocation of the exact layout.
    unsafe { p.write(StringAggState { data, len: 0, maxlen: INITIAL_SIZE as u32, cursor: 0 }) };
    Ok(p)
}

pub fn string_agg_transfn(fcinfo: &mut Fcinfo) -> PgResult<*mut StringAggState> {
    let [a, b, c] = *fcinfo.args_n::<3>();
    let mut state: *mut StringAggState =
        if a.isnull { core::ptr::null_mut() } else { a.value.as_usize() as *mut StringAggState };

    if !b.isnull {
        // SAFETY: context, if set, is the evaltrans build's AggStateNode,
        // live across every call through this frame.
        let Some(agg_mcx) = (unsafe { fcinfo.agg_context() }) else {
            return Err(non_aggregate_context());
        };
        let mut isfirst = false;
        if state.is_null() {
            state = make_string_agg_state(agg_mcx)?;
            isfirst = true;
        }
        // SAFETY: a non-null state is the aggcontext-lived value this transfn
        // chain returned; no other reference is live during the call.
        let st = unsafe { &mut *state };
        if !c.isnull {
            // SAFETY: a non-null arg is a live text/bytea varlena.
            let delim = unsafe { fcinfo.arg_varlena_packed(2)? }.data();
            st.append(agg_mcx, delim)?;
            if isfirst {
                st.cursor = delim.len() as u32;
            }
        }
        // SAFETY: a non-null arg is a live text/bytea varlena.
        let value = unsafe { fcinfo.arg_varlena_packed(1)? }.data();
        st.append(agg_mcx, value)?;
    }
    Ok(state)
}

// string_agg_combine (varlena.c): merges a sibling worker's partial state
// into this one, copying into agg_mcx on first contact.
pub fn string_agg_combine(fcinfo: &mut Fcinfo) -> PgResult<*mut StringAggState> {
    let [a, b] = *fcinfo.args_n::<2>();
    // SAFETY: combine is only ever invoked with a live AggStateNode context.
    let Some(agg_mcx) = (unsafe { fcinfo.agg_context() }) else {
        return Err(non_aggregate_call_context());
    };
    let state1: *mut StringAggState =
        if a.isnull { core::ptr::null_mut() } else { a.value.as_usize() as *mut StringAggState };
    let state2: *const StringAggState =
        if b.isnull { core::ptr::null() } else { b.value.as_usize() as *const StringAggState };

    if state2.is_null() {
        return Ok(state1);
    }
    // SAFETY: a non-null state2 is a live partial state from a sibling
    // worker; read-only here.
    let s2 = unsafe { &*state2 };
    if state1.is_null() {
        let new_state = make_string_agg_state(agg_mcx)?;
        // SAFETY: freshly allocated above; no other reference exists.
        let st = unsafe { &mut *new_state };
        st.append(agg_mcx, s2.accumulated())?;
        st.cursor = s2.cursor;
        return Ok(new_state);
    }
    if !s2.accumulated().is_empty() {
        // SAFETY: a non-null state1 is the aggcontext-lived accumulator;
        // state2 is a distinct read-only allocation.
        let st1 = unsafe { &mut *state1 };
        st1.append(agg_mcx, s2.accumulated())?;
    }
    Ok(state1)
}

// string_agg_serialize (varlena.c): wire = int32 cursor, then raw data.
pub fn string_agg_serialize<'mcx>(mcx: Mcx<'mcx>, state: &StringAggState) -> PgResult<Bytea<'mcx>> {
    let mut buf = ::pqformat::pq_begintypsend(mcx)?;
    ::pqformat::pq_sendint(&mut buf, state.cursor, 4)?;
    ::pqformat::pq_sendbytes(&mut buf, state.accumulated())?;
    Ok(::pqformat::pq_endtypsend(buf))
}

// string_agg_deserialize (varlena.c): inverse of string_agg_serialize.
pub fn string_agg_deserialize(agg_mcx: Mcx<'_>, payload: &[u8]) -> PgResult<*mut StringAggState> {
    let mut buf = ::stringinfo::StringInfo::with_capacity_in(agg_mcx, payload.len() + 1)?;
    buf.append_bytes(payload)?;
    let cursor = ::pqformat::pq_getmsgint(&mut buf, 4)?;
    let datalen = payload.len() - 4;
    let data = ::pqformat::pq_getmsgbytes(&mut buf, datalen)?;
    let result = make_string_agg_state(agg_mcx)?;
    // SAFETY: freshly allocated above; no other reference exists.
    let st = unsafe { &mut *result };
    st.cursor = cursor;
    st.append(agg_mcx, data)?;
    ::pqformat::pq_getmsgend(&buf)?;
    Ok(result)
}

pub fn string_agg_finalfn(fcinfo: &Fcinfo) -> Option<&[u8]> {
    let a = fcinfo.args_n::<1>()[0];
    if a.isnull {
        return None;
    }
    // SAFETY: a non-null arg0 is the aggcontext-lived state (transfn contract),
    // read-only here.
    let st = unsafe { &*(a.value.as_usize() as *const StringAggState) };
    Some(&st.accumulated()[st.cursor as usize..])
}
