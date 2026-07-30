// explain_dr.c — the EXPLAIN (SERIALIZE) DestReceiver (PG 18.3).
#![allow(non_snake_case)]

use core::ffi::CStr;

use ::mcx::{Mcx, MemoryContext, PgVec};
use ::pqformat::{
    pq_beginmessage_reuse, pq_sendbytes, pq_sendcountedtext, pq_sendint16, pq_sendint32,
};
use ::stringinfo::StringInfo;
use ::types_core::instrument::{instr_time, BufferUsage};
use ::types_core::primitive::InvalidOid;
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use ::types_fmgr::{function_call1_coll_in, send_function_call, FmgrInfo, PackedVarlena};
use ::types_slot::SlotData;
use ::types_tuple::TupleDescData;

#[cfg(test)]
mod tests;

const PQMSG_DATA_ROW: u8 = b'D';

#[derive(Clone, Copy, Default)]
pub struct SerializeMetrics {
    pub bytesSent: u64,
    pub timeSpent: instr_time,
    pub bufferUsage: BufferUsage,
}

// C's receiver dereferences its ExplainState* for es->serialize/timing/buffers;
// those are fixed by ExplainOnePlan before execution, so they're captured here
// at creation (a live &ExplainState would cycle this crate into explain).
pub struct SerializeDestReceiver<'mcx> {
    mcx: Mcx<'mcx>,
    format: i8,
    timing: bool,
    buffers: bool,
    // C compares the TupleDesc pointer; the Rc's address is the same token.
    attrinfo: usize,
    nattrs: i32,
    finfos: Option<PgVec<'mcx, FmgrInfo>>,
    buf: Option<StringInfo<'mcx>>,
    tmpcontext: Option<MemoryContext>,
    pub metrics: SerializeMetrics,
}

// C: CreateExplainSerializeDestReceiver; format resolved from es->serialize
// here rather than in rStartup (see struct comment).
pub fn CreateExplainSerializeDestReceiver<'mcx>(
    mcx: Mcx<'mcx>,
    binary: bool,
    timing: bool,
    buffers: bool,
) -> SerializeDestReceiver<'mcx> {
    SerializeDestReceiver {
        mcx,
        format: binary as i8,
        timing,
        buffers,
        attrinfo: 0,
        nattrs: 0,
        finfos: None,
        buf: None,
        tmpcontext: None,
        metrics: SerializeMetrics::default(),
    }
}

impl<'mcx> SerializeDestReceiver<'mcx> {
    pub fn startup(&mut self, _operation: i32, _typeinfo: &TupleDescData<'_>) -> PgResult<()> {
        self.tmpcontext = Some(MemoryContext::new_bump("SerializeTupleReceive"));
        self.buf = Some(StringInfo::new_in(self.mcx)?);
        self.metrics = SerializeMetrics::default();
        Ok(())
    }

    // C: serialize_prepare_info — printtup_prepare_info minus per-column
    // format variance.
    fn prepare_info(
        &mut self,
        typeinfo: &TupleDescData<'_>,
        token: usize,
        nattrs: i32,
    ) -> PgResult<()> {
        self.finfos = None;
        self.attrinfo = token;
        self.nattrs = nattrs;
        if nattrs <= 0 {
            return Ok(());
        }

        let mut finfos: PgVec<'mcx, FmgrInfo> = PgVec::new_in(self.mcx);
        finfos
            .try_reserve_exact(nattrs as usize)
            .map_err(|_| self.mcx.oom(nattrs as usize * core::mem::size_of::<FmgrInfo>()))?;
        for i in 0..nattrs as usize {
            let attr = typeinfo.attr(i);
            let fn_oid = match self.format {
                0 => lsyscache_seams::get_type_output_info::call(attr.atttypid)?.0,
                1 => lsyscache_seams::get_type_binary_output_info::call(attr.atttypid)?.0,
                other => return Err(unsupported_format_code(other)),
            };
            finfos.push(fmgr_seams::fmgr_info::call(fn_oid)?);
        }
        self.finfos = Some(finfos);
        Ok(())
    }

    // C: serializeAnalyzeReceive — printtup() plus measurement, minus the send.
    pub fn receive_slot(&mut self, slot: &mut SlotData<'mcx>) -> PgResult<bool> {
        let mut start = instr_time::default();
        if self.timing {
            start = instrument::instr_time_current();
        }
        let mut instr_start = BufferUsage::default();
        if self.buffers {
            instr_start = instrument::pg_buffer_usage();
        }

        {
            let desc = slot
                .base()
                .tts_tupleDescriptor
                .as_ref()
                .expect("serializeAnalyzeReceive: slot without descriptor");
            let token = std::rc::Rc::as_ptr(desc) as usize;
            if self.attrinfo != token || self.nattrs != desc.natts {
                self.prepare_info(desc, token, desc.natts)?;
            }
        }
        exectuples::slot_getallattrs(slot);

        let base = slot.base();
        let buf = self
            .buf
            .as_mut()
            .expect("serializeAnalyzeReceive before startup");
        let tmpctx = self
            .tmpcontext
            .as_mut()
            .expect("serializeAnalyzeReceive before startup");
        let finfos: &mut [FmgrInfo] = match &mut self.finfos {
            Some(v) => &mut v[..],
            None => &mut [],
        };
        let natts = self.nattrs as usize;

        pq_beginmessage_reuse(buf, PQMSG_DATA_ROW);
        pq_sendint16(buf, natts as u16)?;

        for i in 0..natts {
            if base.tts_isnull[i] {
                pq_sendint32(buf, (-1i32) as u32)?;
                continue;
            }
            let attr = base.tts_values[i];
            let finfo = &mut finfos[i];

            if self.format == 0 {
                let out = function_call1_coll_in(finfo, InvalidOid, tmpctx.mcx(), attr)?;
                // SAFETY: text output fns return a NUL-terminated cstring
                // datum (the contract C's DatumGetCString trusts).
                let s = unsafe { CStr::from_ptr(out.as_usize() as *const core::ffi::c_char) }
                    .to_bytes();
                pq_sendcountedtext(buf, s)?;
            } else {
                let out = send_function_call(finfo, attr, tmpctx.mcx())?;
                // SAFETY: send fns return an untoasted bytea image (C's
                // DatumGetByteaP); external/compressed panics in from_ptr.
                let v = unsafe { PackedVarlena::from_ptr(out.as_usize() as *const u8) };
                let data = v.data();
                pq_sendint32(buf, data.len() as u32)?;
                pq_sendbytes(buf, data)?;
            }
        }

        // C never pq_endmessage_reuse()s here — the row must not reach the
        // client; count it and let the next row's beginmessage reset the buf.
        self.metrics.bytesSent += buf.len() as u64;

        tmpctx.reset();

        if self.timing {
            let end = instrument::instr_time_current();
            self.metrics.timeSpent.accum_diff(end, start);
        }
        if self.buffers {
            let now = instrument::pg_buffer_usage();
            instrument::buffer_usage_accum_diff(&mut self.metrics.bufferUsage, &now, &instr_start);
        }

        Ok(true)
    }

    pub fn shutdown(&mut self) {
        self.finfos = None;
        self.attrinfo = 0;
        self.buf = None;
        self.tmpcontext = None;
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn unsupported_format_code(format: i8) -> Box<PgError> {
    PgError::error(format!("unsupported format code: {format}"))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
        .into()
}

pub fn init_seams() {}
