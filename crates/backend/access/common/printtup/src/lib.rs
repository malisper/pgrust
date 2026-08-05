// printtup.c — the DestRemote/DestRemoteExecute per-row output path (PG 18.3).
#![allow(non_snake_case)]

use core::cell::Cell;
use core::ffi::CStr;
use std::rc::Rc;

use ::datum::Datum;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::pqformat::{
    pq_beginmessage_reuse, pq_endmessage_reuse, pq_sendbytes, pq_sendcountedtext, pq_sendint16,
    pq_sendint32, pq_writeint16, pq_writeint32, pq_writestring,
};
use ::pquery_seams::TargetEntrySummary;
use ::stringinfo::StringInfo;
use ::types_core::{primitive::InvalidOid, FirstNormalObjectId, Oid, NAMEDATALEN};
use ::types_dest::CommandDest;
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use ::types_fmgr::{
    function_call1_coll, function_call1_coll_in, send_function_call, FmgrInfo, PackedVarlena,
};
use ::types_portal::Portal;
use ::types_slot::SlotData;
use ::types_tuple::TupleDescData;

pub mod debugtup;
pub mod printsimple;

#[cfg(test)]
mod tests;

const PQMSG_ROW_DESCRIPTION: u8 = b'T';
const PQMSG_DATA_ROW: u8 = b'D';
// MAX_CONVERSION_GROWTH (mb/pg_wchar.h).
const MAX_CONVERSION_GROWTH: usize = 4;

pub struct PrinttupAttrInfo {
    pub typoutput: Oid,
    pub typsend: Oid,
    pub typisvarlena: bool,
    pub format: i16,
    pub finfo: FmgrInfo,
}

pub struct DrPrinttup<'mcx> {
    pub mydest: CommandDest,
    pub sendDescrip: bool,
    portal: Option<Portal<'mcx>>,
    buf: Option<StringInfo<'static>>,
    // C compares the TupleDesc pointer; the Rc's address is the same token.
    attrinfo: usize,
    nattrs: i32,
    myinfo: Option<PgVec<'static, PrinttupAttrInfo>>,
    conv_needed: bool,
    // C's per-row tmpcontext, but only where a column can allocate per row:
    // binary send byteas, or text-lane varlenas (short-header args detoast-
    // expand into the armed frame; out-fn results stay retained scratch).
    // Reset after each row; must be a bump context — the row's allocations
    // are reclaimed wholesale, never freed (exact-accounting backends assert
    // that as a leak). None until such a column appears.
    send_ctx: Option<MemoryContext>,
}

// C allocates the wire buf/myinfo in es_query_cxt per executor (printtup.c:120,
// execMain.c:330); here the receiver outlives any query context lifetime it
// could borrow, so it owns its scratch: a backend-lifetime context plus a
// pooled wire buffer whose capacity is retained across statements (rule 7).
fn scratch_mcx() -> Mcx<'static> {
    thread_local! {
        static CTX: Cell<Option<&'static MemoryContext>> = const { Cell::new(None) };
    }
    CTX.with(|c| match c.get() {
        Some(m) => m.mcx(),
        None => {
            let m: &'static MemoryContext =
                ::mcx::session_root("PrinttupScratch");
            // LIFO: drop the pooled wire buffer before its context is freed
            // (Cell<Option<StringInfo>> is a droppy TLS payload).
            ::mcx::register_session_cleanup(Box::new(|| {
                WIRE_BUF.with(|c| drop(c.take()));
            }));
            c.set(Some(m));
            m.mcx()
        }
    })
}

thread_local! {
    static WIRE_BUF: Cell<Option<StringInfo<'static>>> = const { Cell::new(None) };
}

pub(crate) fn take_wire_buf() -> PgResult<StringInfo<'static>> {
    match WIRE_BUF.with(Cell::take) {
        Some(buf) => Ok(buf),
        None => StringInfo::new_in(scratch_mcx()),
    }
}

pub(crate) fn put_wire_buf(mut buf: StringInfo<'static>) {
    buf.reset();
    WIRE_BUF.with(|c| c.set(Some(buf)));
}

pub fn printtup_create_DR<'mcx>(dest: CommandDest) -> DrPrinttup<'mcx> {
    DrPrinttup {
        mydest: dest,
        sendDescrip: dest == CommandDest::Remote,
        portal: None,
        buf: None,
        attrinfo: 0,
        nattrs: 0,
        myinfo: None,
        conv_needed: false,
        send_ctx: None,
    }
}

pub fn SetRemoteDestReceiverParams<'mcx>(myState: &mut DrPrinttup<'mcx>, portal: Portal<'mcx>) {
    debug_assert!(matches!(
        myState.mydest,
        CommandDest::Remote | CommandDest::RemoteExecute
    ));
    myState.portal = Some(portal);
}

impl<'mcx> DrPrinttup<'mcx> {
    pub fn startup(&mut self, _operation: i32, typeinfo: &TupleDescData<'_>) -> PgResult<()> {
        // Reused across all rows (docs/optimizations/printtup-parity.md).
        let mut buf = take_wire_buf()?;
        if self.sendDescrip {
            let mcx = scratch_mcx();
            let portal = self
                .portal
                .as_ref()
                .expect("printtup_startup: no portal set")
                .borrow();
            let targetlist = pquery_seams::fetch_portal_target_list::call(mcx, &portal)?;
            let formats = (!portal.formats.is_empty()).then_some(&portal.formats[..]);
            SendRowDescriptionMessage(&mut buf, typeinfo, &targetlist, formats)?;
        }
        self.buf = Some(buf);
        Ok(())
    }

    fn prepare_info(
        &mut self,
        typeinfo: &TupleDescData<'_>,
        token: usize,
        numAttrs: i32,
    ) -> PgResult<()> {
        self.myinfo = None;
        self.attrinfo = token;
        self.nattrs = numAttrs;
        if numAttrs <= 0 {
            return Ok(());
        }
        // Conversion-needed resolved once here, never in the row loop
        // (strategy lever 2; the pqformat benchmark record's watch item).
        self.conv_needed = mbutils_seams::server_to_client_conversion_needed::call();

        let mcx = scratch_mcx();
        let portal = self
            .portal
            .as_ref()
            .expect("printtup: no portal set")
            .borrow();
        let formats = (!portal.formats.is_empty()).then_some(&portal.formats[..]);
        // Droppy payload (FmgrInfo's fn_extra), so PgVec::new_in rather than
        // the !needs_drop capacity helper; resolve-once, never per row.
        let mut info: PgVec<'static, PrinttupAttrInfo> = PgVec::new_in(mcx);
        info.try_reserve_exact(numAttrs as usize)
            .map_err(|_| mcx.oom(numAttrs as usize * core::mem::size_of::<PrinttupAttrInfo>()))?;
        for i in 0..numAttrs as usize {
            let format = formats.map_or(0, |f| f[i]);
            let attr = typeinfo.attr(i);
            let entry = match format {
                0 => {
                    let (typoutput, typisvarlena) =
                        lsyscache_seams::get_type_output_info::call(attr.atttypid)?;
                    PrinttupAttrInfo {
                        typoutput,
                        typsend: InvalidOid,
                        typisvarlena,
                        format,
                        finfo: fmgr_seams::fmgr_info::call(typoutput)?,
                    }
                }
                1 => {
                    let (typsend, typisvarlena) =
                        lsyscache_seams::get_type_binary_output_info::call(attr.atttypid)?;
                    PrinttupAttrInfo {
                        typoutput: InvalidOid,
                        typsend,
                        typisvarlena,
                        format,
                        finfo: fmgr_seams::fmgr_info::call(typsend)?,
                    }
                }
                _ => return Err(unsupported_format_code(format)),
            };
            info.push(entry);
        }
        // User-defined (non-builtin) output fns follow the result-mcx
        // convention even for fixed-length results; builtin cstring kernels
        // return FmgrInfo scratch and stay off the context path.
        if info
            .iter()
            .any(|e| e.format == 1 || e.typisvarlena || e.finfo.fn_oid >= FirstNormalObjectId)
        {
            if self.send_ctx.is_none() {
                self.send_ctx = Some(MemoryContext::new_bump("PrinttupSend"));
            }
        } else {
            self.send_ctx = None;
        }
        self.myinfo = Some(info);
        Ok(())
    }

    // C: printtup(); false never returned (C returns true unconditionally).
    pub fn receive_slot(&mut self, slot: &mut SlotData<'mcx>) -> PgResult<bool> {
        {
            let desc = slot
                .base()
                .tts_tupleDescriptor
                .as_ref()
                .expect("printtup: slot without descriptor");
            let token = Rc::as_ptr(desc) as usize;
            if self.attrinfo != token || self.nattrs != desc.natts {
                self.prepare_info(desc, token, desc.natts)?;
            }
        }
        exectuples::slot_getallattrs(slot);

        let base = slot.base();
        let buf = self.buf.as_mut().expect("printtup before printtup_startup");
        let myinfo: &mut [PrinttupAttrInfo] = match &mut self.myinfo {
            Some(v) => &mut v[..],
            None => &mut [],
        };
        let mut send_ctx = self.send_ctx.as_mut();
        let natts = self.nattrs as usize;

        pq_beginmessage_reuse(buf, PQMSG_DATA_ROW);
        pq_sendint16(buf, natts as u16)?;

        for i in 0..natts {
            if base.tts_isnull[i] {
                pq_sendint32(buf, (-1i32) as u32)?;
                continue;
            }
            let attr = base.tts_values[i];
            let thisState = &mut myinfo[i];

            if thisState.format == 0 {
                let out = match send_ctx.as_deref() {
                    Some(ctx) => {
                        function_call1_coll_in(&mut thisState.finfo, InvalidOid, ctx.mcx(), attr)?
                    }
                    None => output_function_call(&mut thisState.finfo, attr)?,
                };
                // SAFETY: text output fns return a NUL-terminated cstring
                // datum (the contract C's DatumGetCString trusts).
                let s =
                    unsafe { CStr::from_ptr(out.as_usize() as *const core::ffi::c_char) }
                        .to_bytes();
                if self.conv_needed {
                    pq_sendcountedtext(buf, s)?;
                } else {
                    pq_sendint32(buf, s.len() as u32)?;
                    buf.append_bytes_nt(s)?;
                }
            } else {
                let mcx = send_ctx
                    .as_deref()
                    .expect("printtup: binary column without send_ctx")
                    .mcx();
                let out = send_function_call(&mut thisState.finfo, attr, mcx)?;
                // SAFETY: send fns return an untoasted bytea image (C's
                // DatumGetByteaP); external/compressed panics in from_ptr.
                let v = unsafe { PackedVarlena::from_ptr(out.as_usize() as *const u8) };
                let data = v.data();
                pq_sendint32(buf, data.len() as u32)?;
                pq_sendbytes(buf, data)?;
            }
        }

        pq_endmessage_reuse(buf)?;
        // C resets the per-row tmpcontext here; the byteas were copied above.
        if let Some(ctx) = send_ctx.as_deref_mut() {
            ctx.reset();
        }
        Ok(true)
    }

    pub fn shutdown(&mut self) {
        self.myinfo = None;
        self.attrinfo = 0;
        if let Some(buf) = self.buf.take() {
            put_wire_buf(buf);
        }
    }
}

// C: OutputFunctionCall/SendFunctionCall == FunctionCall1 over the resolved
// carrier; one stack fcinfo, args written in place, isnull checked.
#[inline]
fn output_function_call(finfo: &mut FmgrInfo, val: Datum) -> PgResult<Datum> {
    function_call1_coll(finfo, InvalidOid, val)
}

pub fn SendRowDescriptionMessage(
    buf: &mut StringInfo<'_>,
    typeinfo: &TupleDescData<'_>,
    targetlist: &[TargetEntrySummary],
    formats: Option<&[i16]>,
) -> PgResult<()> {
    let natts = typeinfo.natts as usize;
    pq_beginmessage_reuse(buf, PQMSG_ROW_DESCRIPTION);
    pq_sendint16(buf, natts as u16)?;

    // C preallocates the full message so the unchecked pq_write* inlines run;
    // here the same reserve makes every write's grow check a dead branch.
    buf.enlarge((NAMEDATALEN as usize * MAX_CONVERSION_GROWTH + 4 + 2 + 4 + 2 + 4 + 2) * natts)?;

    let mut tlist_item = 0usize;
    for i in 0..natts {
        let att = typeinfo.attr(i);
        let (atttypid, atttypmod) =
            lsyscache_seams::get_base_type_and_typmod::call(att.atttypid, att.atttypmod)?;

        while targetlist.get(tlist_item).is_some_and(|t| t.resjunk) {
            tlist_item += 1;
        }
        let (resorigtbl, resorigcol) = match targetlist.get(tlist_item) {
            Some(tle) => {
                tlist_item += 1;
                (tle.resorigtbl, tle.resorigcol)
            }
            None => (0, 0),
        };
        let format = formats.map_or(0, |f| f[i]);

        pq_writestring(buf, att.attname.name_str())?;
        pq_writeint32(buf, resorigtbl);
        pq_writeint16(buf, resorigcol as u16);
        pq_writeint32(buf, atttypid);
        pq_writeint16(buf, att.attlen as u16);
        pq_writeint32(buf, atttypmod as u32);
        pq_writeint16(buf, format as u16);
    }

    pq_endmessage_reuse(buf)
}

#[track_caller]
#[cold]
#[inline(never)]
fn unsupported_format_code(format: i16) -> Box<PgError> {
    PgError::error(format!("unsupported format code: {format}"))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
        .into()
}

pub fn init_seams() {}
