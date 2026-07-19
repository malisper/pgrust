// printtup.c debugStartup/debugtup/printatt (printtup.c:419-489): the
// DestDebug per-row output path for the standalone (--single) backend.
// Everything prints to stdout, as C's printf does.

use std::io::Write;

use ::datum::Datum;
use ::mcx::MemoryContext;
use ::types_core::primitive::InvalidOid;
use ::types_error::PgResult;
use ::types_fmgr::function_call1_coll_in;
use ::types_slot::SlotData;
use ::types_tuple::TupleDescData;

/// DR_printtup's debug sibling. C's debugtup resolves the output function per
/// row per column (getTypeOutputInfo in the loop) — kept, it is the cold
/// interactive path. The bump context arms the per-row output-function
/// allocations and is reset after each row (printtup's send_ctx convention).
pub struct DrDebugtup {
    out_ctx: Option<MemoryContext>,
}

impl Default for DrDebugtup {
    fn default() -> Self {
        Self::new()
    }
}

pub fn debugtup_create_DR() -> DrDebugtup {
    DrDebugtup::new()
}

impl DrDebugtup {
    pub fn new() -> Self {
        DrDebugtup { out_ctx: None }
    }

    // debugStartup (printtup.c:444): show the return type of the tuples.
    pub fn startup(&mut self, _operation: i32, typeinfo: &TupleDescData<'_>) -> PgResult<()> {
        let mut out = std::io::stdout().lock();
        for i in 0..typeinfo.natts as usize {
            printatt(&mut out, i as u32 + 1, typeinfo, i, None);
        }
        let _ = writeln!(out, "\t----");
        Ok(())
    }

    // debugtup (printtup.c:462): print one tuple for an interactive backend.
    pub fn receive_slot(&mut self, slot: &mut SlotData<'_>) -> PgResult<bool> {
        exectuples::slot_getallattrs(slot);

        if self.out_ctx.is_none() {
            self.out_ctx = Some(MemoryContext::new_bump("DebugtupOutput"));
        }
        let out_ctx = self.out_ctx.as_mut().expect("just set");

        let base = slot.base();
        let typeinfo = base
            .tts_tupleDescriptor
            .as_ref()
            .expect("debugtup: slot without descriptor")
            .clone();
        let natts = typeinfo.natts as usize;

        let mut out = std::io::stdout().lock();
        for i in 0..natts {
            if base.tts_isnull[i] {
                continue;
            }
            let attr: Datum = base.tts_values[i];
            let (typoutput, _typisvarlena) =
                lsyscache_seams::get_type_output_info::call(typeinfo.attr(i).atttypid)?;

            // OidOutputFunctionCall(typoutput, attr): resolve + call; the
            // cstring result lands in the per-row bump context.
            let mut finfo = fmgr_seams::fmgr_info::call(typoutput)?;
            let value = function_call1_coll_in(&mut finfo, InvalidOid, out_ctx.mcx(), attr)?;
            // SAFETY: text output fns return a NUL-terminated cstring datum
            // (the contract C's DatumGetCString trusts).
            let s = unsafe {
                core::ffi::CStr::from_ptr(value.as_usize() as *const core::ffi::c_char)
            }
            .to_bytes();
            printatt(&mut out, i as u32 + 1, &typeinfo, i, Some(s));
        }
        let _ = writeln!(out, "\t----");
        drop(out);
        self.out_ctx.as_mut().expect("just set").reset();
        Ok(true)
    }

    pub fn shutdown(&mut self) {
        self.out_ctx = None;
    }
}

// printatt (printtup.c:423).
fn printatt(
    out: &mut impl Write,
    attribute_id: u32,
    typeinfo: &TupleDescData<'_>,
    i: usize,
    value: Option<&[u8]>,
) {
    let att = typeinfo.attr(i);
    let _ = writeln!(
        out,
        "\t{:2}: {}{}{}{}\t(typeid = {}, len = {}, typmod = {}, byval = {})",
        attribute_id,
        String::from_utf8_lossy(att.attname.name_str()),
        if value.is_some() { " = \"" } else { "" },
        value.map(|v| String::from_utf8_lossy(v).into_owned()).unwrap_or_default(),
        if value.is_some() { "\"" } else { "" },
        att.atttypid,
        att.attlen,
        att.atttypmod,
        if att.attbyval { 't' } else { 'f' },
    );
}
