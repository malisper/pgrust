//! Port of `src/backend/access/common/printtup.c` (PostgreSQL 18.3) — routines
//! to print out tuples to the destination (frontend clients and standalone
//! backends).
//!
//! The full translation unit is ported here: the `DestReceiver` lifecycle
//! (`printtup_create_DR`, `SetRemoteDestReceiverParams`, `printtup_startup`,
//! `printtup`, `printtup_shutdown`, `printtup_destroy`),
//! `SendRowDescriptionMessage`, `printtup_prepare_info`, and the interactive
//! `debugStartup` / `debugtup` / `printatt` helpers.
//!
//! The RowDescription / DataRow message bytes are built in this crate with the
//! ported `backend-libpq-pqformat` send-side primitives, exactly as the C code
//! uses `pqformat.h`. The send side is over a `StringInfo` charged to the
//! threaded `Mcx`; pqformat's `pq_endmessage_reuse` already routes to the
//! protocol sink (`pq_putmessage`), so there is no `comm()` seam here. The C
//! pre-reservation step (`enlargeStringInfo` + the inline `pq_writeintNN`
//! writers) is a pure speed optimisation; the charged `pq_sendXXX` appends grow
//! the buffer identically without it.
//!
//! The per-column descriptor (`TupleDesc`) is the reused owned
//! [`types_tuple::heaptuple::TupleDescData`]: `TupleDescAttr` / `natts` /
//! `attbyval` are pure in-process reads done directly. C's
//! `myState->attrinfo == typeinfo` identity check compares the descriptor
//! *pointer*; the owned model records the borrowed descriptor's address as an
//! opaque identity token (never dereferenced — only compared) to reproduce the
//! "did the slot's descriptor change?" trigger exactly.
//!
//! Only genuinely-external subsystems go through the [`PrinttupRuntime`] trait
//! (stateful per-receiver, with fail-safe defaults): the executor
//! `TupleTableSlot`, the catalog type-output lookups (`lsyscache.c`), the fmgr
//! calling convention (`fmgr.c`), the Portal target-list / format codes
//! (`pquery.c`), and the per-row `tmpcontext` (`mcxt.c`).

#![forbid(unsafe_code)]
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use mcx::Mcx;
use types_core::{FmgrInfo, Oid};
use types_datum::datum::Datum;
use types_dest::dest::CommandDest;
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use types_stringinfo::StringInfo;
use types_tuple::heaptuple::{FormData_pg_attribute, TupleDescData};

use backend_libpq_pqformat::{
    pq_beginmessage_reuse, pq_endmessage_reuse, pq_sendbytes, pq_sendcountedtext, pq_sendint16,
    pq_sendint32, pq_sendstring,
};

/// `PqMsg_RowDescription` (`libpq/protocol.h`).
pub const PqMsg_RowDescription: u8 = b'T';
/// `PqMsg_DataRow` (`libpq/protocol.h`).
pub const PqMsg_DataRow: u8 = b'D';

/// `pg_attribute.h`: `NAMEDATALEN`.
pub const NAMEDATALEN: usize = 64;

/// `MAX_CONVERSION_GROWTH` (`mb/pg_wchar.h`).
pub const MAX_CONVERSION_GROWTH: i32 = 4;

/// C: `PrinttupAttrInfo` — per-attribute output info. `finfo` is the lookup
/// info for either `typoutput` or `typsend`, whichever we are using.
#[derive(Clone, Debug)]
pub struct PrinttupAttrInfo {
    /// Oid for the type's text output fn.
    pub typoutput: Oid,
    /// Oid for the type's binary output fn.
    pub typsend: Oid,
    /// Is it varlena (ie possibly toastable)?
    pub typisvarlena: bool,
    /// Format code for this column.
    pub format: i16,
    /// Precomputed call info for output fn.
    pub finfo: FmgrInfo,
}

impl Default for PrinttupAttrInfo {
    fn default() -> Self {
        Self {
            typoutput: 0,
            typsend: 0,
            typisvarlena: false,
            format: 0,
            finfo: FmgrInfo::empty(),
        }
    }
}

/// C's `DR_printtup.attrinfo` is a `TupleDesc` pointer that `printtup` compares
/// for raw equality against the slot's descriptor to decide whether the cached
/// per-attribute info needs re-deriving. The owned model records the borrowed
/// descriptor's address as a plain token (never dereferenced).
fn descriptor_identity(typeinfo: &TupleDescData) -> usize {
    core::ptr::from_ref(typeinfo) as *const () as usize
}

/// C: `DR_printtup` — private state for a printtup destination object. The I/O
/// buffer, the per-row `tmpcontext` and the Portal belong to external
/// subsystems (reached via [`PrinttupRuntime`] / caller); what remains is the
/// receiver bookkeeping printtup.c owns directly.
pub struct DR_printtup {
    /// `mydest` — the `CommandDest` this receiver targets.
    pub mydest: CommandDest,
    /// Send RowDescription at startup?
    pub sendDescrip: bool,
    /// The attr info we are set up for (C: `TupleDesc attrinfo`), as the
    /// descriptor *identity token*. `None` means "not set up".
    attrinfo: Option<usize>,
    /// Cached info about each attr (C: `PrinttupAttrInfo *myinfo`). Empty means
    /// `myinfo == NULL`.
    pub myinfo: Vec<PrinttupAttrInfo>,
    /// Number of attrs we are set up for (C: `nattrs`).
    pub nattrs: i32,
}

impl DR_printtup {
    /// C: `printtup_create_DR(CommandDest dest)` — create a DestReceiver for
    /// printtup. The C function also installs `printtup` as `receiveSlot` and
    /// the lifecycle hooks; in this port those are the free functions below.
    /// Sends a T message automatically if `DestRemote`, not if
    /// `DestRemoteExecute`.
    pub fn printtup_create_DR(dest: CommandDest) -> Self {
        Self {
            mydest: dest,
            sendDescrip: dest == CommandDest::Remote,
            attrinfo: None,
            myinfo: Vec::new(),
            nattrs: 0,
        }
    }

    /// True if the receiver's cached descriptor identity matches `typeinfo`
    /// (C: `myState->attrinfo == typeinfo`).
    pub fn attrinfo_matches(&self, typeinfo: &TupleDescData) -> bool {
        self.attrinfo == Some(descriptor_identity(typeinfo))
    }
}

/// C: a `TargetEntry`'s printtup-relevant fields (`nodes/primnodes.h`).
/// `SendRowDescriptionMessage` walks the Portal target list, skipping resjunk
/// entries, and reads `resorigtbl` / `resorigcol`. The list lives in the
/// parser/planner node tree; the runtime hands us this projection.
#[derive(Clone, Copy, Debug, Default)]
pub struct TargetEntryInfo {
    /// C: `TargetEntry->resjunk`.
    pub resjunk: bool,
    /// C: `TargetEntry->resorigtbl`.
    pub resorigtbl: Oid,
    /// C: `TargetEntry->resorigcol`.
    pub resorigcol: i16,
}

/// Result of a text output call (`OutputFunctionCall`) — the NUL-terminated C
/// string's payload bytes (NUL excluded), matching `strlen(outputstr)`.
pub type OutputStr = Vec<u8>;

/// Result of a binary output call (`SendFunctionCall`) — the `bytea*` payload
/// bytes (`VARDATA` for `VARSIZE - VARHDRSZ` bytes; the runtime strips the
/// varlena header).
pub type OutputBytes = Vec<u8>;

/// Seam over the genuinely-external subsystems printtup drives. The `TupleDesc`
/// is **not** on the seam (it is passed as a parameter, read directly). Every
/// method has a fail-safe default reporting an informative error (or a safe
/// no-op for the memory-context discipline), so an unwired runtime degrades
/// gracefully; the surrounding control flow is ported 1:1.
pub trait PrinttupRuntime {
    /// `portal->formats` (`pquery.c`): the per-column format codes, or `None`
    /// when the array pointer is NULL.
    fn formats(&self) -> PgResult<Option<Vec<i16>>> {
        Err(seam_unavailable("portal->formats"))
    }

    /// `FetchPortalTargetList(portal)` (`pquery.c`), projected to the fields
    /// printtup reads. Empty for the NIL list (a utility statement).
    fn target_list(&self) -> PgResult<Vec<TargetEntryInfo>> {
        Err(seam_unavailable("FetchPortalTargetList"))
    }

    /// `getBaseTypeAndTypmod(atttypid, &atttypmod)` (`lsyscache.c`).
    fn getBaseTypeAndTypmod(&self, atttypid: Oid, atttypmod: &mut i32) -> PgResult<Oid> {
        let _ = (atttypid, atttypmod);
        Err(seam_unavailable("getBaseTypeAndTypmod"))
    }

    /// `getTypeOutputInfo` + `fmgr_info` for a text-format column.
    fn prepare_text(&self, atttypid: Oid) -> PgResult<PrinttupAttrInfo> {
        let _ = atttypid;
        Err(seam_unavailable("getTypeOutputInfo"))
    }

    /// `getTypeBinaryOutputInfo` + `fmgr_info` for a binary-format column.
    fn prepare_binary(&self, atttypid: Oid) -> PgResult<PrinttupAttrInfo> {
        let _ = atttypid;
        Err(seam_unavailable("getTypeBinaryOutputInfo"))
    }

    /// `getTypeOutputInfo(atttypid, &typoutput, &typisvarlena)` used by
    /// `debugtup`. Returns `(typoutput, typisvarlena)`.
    fn get_type_output_info(&self, atttypid: Oid) -> PgResult<(Oid, bool)> {
        let _ = atttypid;
        Err(seam_unavailable("getTypeOutputInfo"))
    }

    /// `slot_getallattrs(slot)` (`execTuples.c`).
    fn slot_getallattrs(&self) -> PgResult<()> {
        Err(seam_unavailable("slot_getallattrs"))
    }

    /// `slot->tts_isnull[attnum]` (`execTuples.c`).
    fn is_null(&self, attnum: usize) -> PgResult<bool> {
        let _ = attnum;
        Err(seam_unavailable("slot->tts_isnull"))
    }

    /// `slot->tts_values[attnum]` (`execTuples.c`).
    fn value(&self, attnum: usize) -> PgResult<Datum> {
        let _ = attnum;
        Err(seam_unavailable("slot->tts_values"))
    }

    /// `OutputFunctionCall(&thisState->finfo, attr)` (`fmgr.c`): text output.
    /// Returns the C string payload bytes (NUL excluded).
    fn output_function_call(&self, finfo: &FmgrInfo, attr: Datum) -> PgResult<OutputStr> {
        let _ = (finfo, attr);
        Err(seam_unavailable("OutputFunctionCall"))
    }

    /// `SendFunctionCall(&thisState->finfo, attr)` (`fmgr.c`): binary output.
    /// Returns the `bytea*` payload (varlena header stripped).
    fn send_function_call(&self, finfo: &FmgrInfo, attr: Datum) -> PgResult<OutputBytes> {
        let _ = (finfo, attr);
        Err(seam_unavailable("SendFunctionCall"))
    }

    /// `OidOutputFunctionCall(typoutput, attr)` (`fmgr.c`), used by `debugtup`.
    fn oid_output_function_call(&self, typoutput: Oid, attr: Datum) -> PgResult<OutputStr> {
        let _ = (typoutput, attr);
        Err(seam_unavailable("OidOutputFunctionCall"))
    }

    /// `MemoryContextSwitchTo(myState->tmpcontext)` (`mcxt.c`): enter the
    /// per-row workspace before the loop. Fail-safe default is a no-op.
    fn enter_tmpcontext(&self) -> PgResult<()> {
        Ok(())
    }

    /// `MemoryContextSwitchTo(oldcontext)` + `MemoryContextReset(tmpcontext)`
    /// (`mcxt.c`): leave/flush the per-row workspace after the loop. Fail-safe
    /// default is a no-op.
    fn exit_tmpcontext(&self) -> PgResult<()> {
        Ok(())
    }

    /// C `printtup_shutdown`: `pfree(myState->buf.data)` +
    /// `MemoryContextDelete(myState->tmpcontext)`, both owned by the runtime.
    fn shutdown_buffers(&self) -> PgResult<()> {
        Ok(())
    }
}

fn seam_unavailable(what: &str) -> PgError {
    PgError::error(format!("printtup runtime seam `{what}` is not installed"))
}

/// C: `SetRemoteDestReceiverParams(DestReceiver *self, Portal portal)`. The
/// portal is held by the runtime (it belongs to `pquery.c`); this records the
/// same `Assert` on `mydest` the C code does.
pub fn SetRemoteDestReceiverParams(self_: &DR_printtup) {
    debug_assert!(
        self_.mydest == CommandDest::Remote || self_.mydest == CommandDest::RemoteExecute
    );
}

/// C: `printtup_startup(DestReceiver *self, int operation, TupleDesc typeinfo)`.
/// Initializes the per-message I/O buffer (returned to the caller; it is
/// re-used across rows and lives outside `tmpcontext`) and, when `sendDescrip`,
/// sends the RowDescription message. `operation` is unused in the C code.
pub fn printtup_startup<'mcx>(
    myState: &DR_printtup,
    mcx: Mcx<'mcx>,
    _operation: i32,
    typeinfo: &TupleDescData,
    runtime: &dyn PrinttupRuntime,
) -> PgResult<StringInfo<'mcx>> {
    // Create I/O buffer to be used for all messages. This cannot be inside
    // tmpcontext, since we want to re-use it across rows. (C: initStringInfo.)
    let mut buf = StringInfo::new_in(mcx);

    // (The C code also creates a "printtup" AllocSet tmpcontext for the per-row
    // workspace; that context is owned/reset by the runtime seam.)

    // If we are supposed to emit row descriptions, then send the tuple
    // descriptor of the tuples.
    if myState.sendDescrip {
        let formats = runtime.formats()?;
        SendRowDescriptionMessage(&mut buf, typeinfo, runtime, formats.as_deref())?;
    }
    Ok(buf)
}

/// C: `SendRowDescriptionMessage(StringInfo buf, TupleDesc typeinfo, List
/// *targetlist, int16 *formats)` — send a RowDescription message to the
/// frontend.
///
/// The targetlist is NIL when executing a utility function without a plan; if
/// non-NIL it is a Query node's targetlist and we ignore resjunk columns. The
/// `formats[]` pointer may be NULL (Describe on a prepared stmt); send zeroes
/// for the format codes then.
pub fn SendRowDescriptionMessage(
    buf: &mut StringInfo<'_>,
    typeinfo: &TupleDescData,
    runtime: &dyn PrinttupRuntime,
    formats: Option<&[i16]>,
) -> PgResult<()> {
    let natts = typeinfo.natts;
    let targetlist = runtime.target_list()?;
    let mut tlist_idx = 0usize;

    // tuple descriptor message type
    pq_beginmessage_reuse(buf, PqMsg_RowDescription);
    // # of attrs in tuples
    pq_sendint16(buf, natts as u16)?;

    for i in 0..natts as usize {
        let att: &FormData_pg_attribute = typeinfo.attr(i);
        let mut atttypid = att.atttypid;
        let mut atttypmod = att.atttypmod;
        let resorigtbl: Oid;
        let resorigcol: i16;
        let format: i16;

        // If column is a domain, send the base type and typmod instead. Lookup
        // before sending any ints, for efficiency.
        atttypid = runtime.getBaseTypeAndTypmod(atttypid, &mut atttypmod)?;

        // Do we have a non-resjunk tlist item?
        while tlist_idx < targetlist.len() && targetlist[tlist_idx].resjunk {
            tlist_idx += 1;
        }
        if tlist_idx < targetlist.len() {
            let tle = &targetlist[tlist_idx];
            resorigtbl = tle.resorigtbl;
            resorigcol = tle.resorigcol;
            tlist_idx += 1;
        } else {
            // No info available, so send zeroes
            resorigtbl = 0;
            resorigcol = 0;
        }

        format = match formats {
            Some(formats) => formats[i],
            None => 0,
        };

        // NameStr(att->attname): the name bytes up to the first NUL.
        // pq_writestring (= pq_sendstring) appends a NUL terminator; the
        // content slice excludes it.
        pq_sendstring(buf, att.attname.name_str())?;
        pq_sendint32(buf, resorigtbl)?;
        pq_sendint16(buf, resorigcol as u16)?;
        pq_sendint32(buf, atttypid)?;
        pq_sendint16(buf, att.attlen as u16)?;
        pq_sendint32(buf, atttypmod as u32)?;
        pq_sendint16(buf, format as u16)?;
    }

    pq_endmessage_reuse(buf);
    Ok(())
}

/// C: `printtup_prepare_info(DR_printtup *myState, TupleDesc typeinfo, int
/// numAttrs)` — get the lookup info that `printtup()` needs. Rejects format
/// codes other than 0 (text) and 1 (binary) with the C
/// `ERRCODE_INVALID_PARAMETER_VALUE` "unsupported format code: %d" error.
pub fn printtup_prepare_info(
    myState: &mut DR_printtup,
    typeinfo: &TupleDescData,
    runtime: &dyn PrinttupRuntime,
    numAttrs: i32,
) -> PgResult<()> {
    let formats = runtime.formats()?;

    // get rid of any old data
    myState.myinfo.clear();

    // C: myState->attrinfo = typeinfo.
    myState.attrinfo = Some(descriptor_identity(typeinfo));
    myState.nattrs = numAttrs;
    if numAttrs <= 0 {
        return Ok(());
    }

    let mut info = Vec::new();
    info.try_reserve(numAttrs as usize)
        .map_err(|_| PgError::error("printtup_prepare_info: out of memory"))?;
    for i in 0..numAttrs as usize {
        let format = formats.as_ref().map(|f| f[i]).unwrap_or(0);
        let attr: &FormData_pg_attribute = typeinfo.attr(i);

        let mut thisState;
        if format == 0 {
            thisState = runtime.prepare_text(attr.atttypid)?;
        } else if format == 1 {
            thisState = runtime.prepare_binary(attr.atttypid)?;
        } else {
            return Err(PgError::error(format!("unsupported format code: {format}"))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
        }
        thisState.format = format;
        info.push(thisState);
    }
    myState.myinfo = info;
    Ok(())
}

/// C: `printtup(TupleTableSlot *slot, DestReceiver *self)` — send a tuple to
/// the client.
///
/// The caller owns the reusable per-message `buf` (created in
/// [`printtup_startup`]) and supplies the slot's `typeinfo`. We re-derive attr
/// info if the slot's `TupleDesc` changed, fully deconstruct the tuple, then
/// build and flush the DataRow bytes. The per-row `tmpcontext` switch is driven
/// structurally around the loop via the runtime.
pub fn printtup(
    myState: &mut DR_printtup,
    buf: &mut StringInfo<'_>,
    typeinfo: &TupleDescData,
    runtime: &dyn PrinttupRuntime,
) -> PgResult<bool> {
    let natts = typeinfo.natts;

    // Set or update my derived attribute info, if needed.
    if !myState.attrinfo_matches(typeinfo) || myState.nattrs != natts {
        printtup_prepare_info(myState, typeinfo, runtime, natts)?;
    }

    // Make sure the tuple is fully deconstructed.
    runtime.slot_getallattrs()?;

    // Switch into per-row context so we can recover memory below
    // (C: oldcontext = MemoryContextSwitchTo(myState->tmpcontext)).
    runtime.enter_tmpcontext()?;

    // Prepare a DataRow message (note buffer is in per-query context).
    pq_beginmessage_reuse(buf, PqMsg_DataRow);
    pq_sendint16(buf, natts as u16)?;

    // send the attributes of this tuple
    for i in 0..natts as usize {
        let attr = runtime.value(i)?;

        if runtime.is_null(i)? {
            pq_sendint32(buf, (-1i32) as u32)?;
            continue;
        }

        // (The C code here runs VALGRIND_CHECK_MEM_IS_DEFINED over the varlena
        // datum when thisState->typisvarlena; a memory-debugging assert with no
        // functional effect and no analogue under safe Rust.)

        let format = myState.myinfo[i].format;
        if format == 0 {
            // Text output
            let outputstr = {
                let finfo = &myState.myinfo[i].finfo;
                runtime.output_function_call(finfo, attr)?
            };
            pq_sendcountedtext(buf, &outputstr)?;
        } else {
            // Binary output
            let outputbytes = {
                let finfo = &myState.myinfo[i].finfo;
                runtime.send_function_call(finfo, attr)?
            };
            pq_sendint32(buf, outputbytes.len() as u32)?;
            pq_sendbytes(buf, &outputbytes)?;
        }
    }

    pq_endmessage_reuse(buf);

    // Return to caller's context, and flush row's temporary memory
    // (C: MemoryContextSwitchTo(oldcontext); MemoryContextReset(tmpcontext)).
    runtime.exit_tmpcontext()?;

    Ok(true)
}

/// C: `printtup_shutdown(DestReceiver *self)`. Frees the cached attr info and
/// the receiver bookkeeping (C: `myState->attrinfo = NULL`); the `buf` and
/// `tmpcontext` are released by the runtime.
pub fn printtup_shutdown(myState: &mut DR_printtup, runtime: &dyn PrinttupRuntime) -> PgResult<()> {
    myState.myinfo.clear();
    myState.nattrs = 0;
    myState.attrinfo = None;
    runtime.shutdown_buffers()
}

/// C: `printtup_destroy(DestReceiver *self)` — `pfree(self)`. The receiver is
/// dropped by its owner; this is the explicit consuming free.
pub fn printtup_destroy(self_: DR_printtup) {
    drop(self_);
}

/// C: `printatt(unsigned attributeId, Form_pg_attribute attributeP, char
/// *value)` — print one attribute for an interactive backend. Returns the
/// formatted line exactly as the C `printf` would emit it.
pub fn printatt(
    attribute_id: u32,
    attribute_p: &FormData_pg_attribute,
    value: Option<&str>,
) -> String {
    let (open, val, close) = match value {
        Some(v) => (" = \"", v, "\""),
        None => ("", "", ""),
    };
    format!(
        "\t{:2}: {}{}{}{}\t(typeid = {}, len = {}, typmod = {}, byval = {})\n",
        attribute_id,
        Latin1Lossy(attribute_p.attname.name_str()),
        open,
        val,
        close,
        attribute_p.atttypid,
        attribute_p.attlen,
        attribute_p.atttypmod,
        if attribute_p.attbyval { 't' } else { 'f' },
    )
}

/// C's `printf` `%s` on a NUL-terminated name renders each byte (Latin-1); ASCII
/// names round-trip exactly. (The descriptor's attname is a valid catalog
/// identifier here.)
struct Latin1Lossy<'a>(&'a [u8]);

impl core::fmt::Display for Latin1Lossy<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        for &b in self.0 {
            f.write_str(core::str::from_utf8(&[b]).unwrap_or("\u{FFFD}"))?;
        }
        Ok(())
    }
}

/// C: `debugStartup(DestReceiver *self, int operation, TupleDesc typeinfo)` —
/// prepare to print tuples for an interactive backend. Returns the accumulated
/// `printf` output (so callers can write it to stdout).
pub fn debugStartup(typeinfo: &TupleDescData, _runtime: &dyn PrinttupRuntime) -> PgResult<String> {
    let natts = typeinfo.natts;
    let mut out = String::new();
    // show the return type of the tuples
    for i in 0..natts as usize {
        let att: &FormData_pg_attribute = typeinfo.attr(i);
        out.push_str(&printatt((i as u32) + 1, att, None));
    }
    out.push_str("\t----\n");
    Ok(out)
}

/// C: `debugtup(TupleTableSlot *slot, DestReceiver *self)` — print one tuple
/// for an interactive backend. Returns the accumulated `printf` output and
/// `true` (matching the C `return true`).
pub fn debugtup(typeinfo: &TupleDescData, runtime: &dyn PrinttupRuntime) -> PgResult<(String, bool)> {
    let natts = typeinfo.natts;
    // debugtup reads one attr at a time via slot_getattr; ensure the slot is
    // deconstructed so the runtime's tts_values/tts_isnull are valid.
    runtime.slot_getallattrs()?;

    let mut out = String::new();
    for i in 0..natts as usize {
        if runtime.is_null(i)? {
            continue;
        }
        let attr = runtime.value(i)?;
        let att: &FormData_pg_attribute = typeinfo.attr(i);
        // getTypeOutputInfo(att->atttypid, &typoutput, &typisvarlena)
        let (typoutput, _typisvarlena) = runtime.get_type_output_info(att.atttypid)?;
        let value = runtime.oid_output_function_call(typoutput, attr)?;
        let rendered = Latin1Lossy(&value);
        out.push_str(&printatt((i as u32) + 1, att, Some(&format!("{rendered}"))));
    }
    out.push_str("\t----\n");
    Ok((out, true))
}

/// This crate owns no seams (its externals are the stateful [`PrinttupRuntime`]
/// trait, supplied per receiver), so `init_seams` is empty.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
