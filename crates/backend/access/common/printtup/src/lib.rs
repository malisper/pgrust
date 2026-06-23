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
//! [`::types_tuple::heaptuple::TupleDescData`]: `TupleDescAttr` / `natts` /
//! `attbyval` are pure in-process reads done directly. C's
//! `myState->attrinfo == typeinfo` identity check compares the descriptor
//! *pointer*; the owned model records the borrowed descriptor's address as an
//! opaque identity token (never dereferenced — only compared) to reproduce the
//! "did the slot's descriptor change?" trigger exactly.
//!
//! The genuinely-external subsystems are reached through their owners' per-owner
//! `-seams` crates (panic-until-installed, no soft fallbacks):
//! * the executor `TupleTableSlot` — `slot_getallattrs` (execTuples.c, via
//!   `backend-executor-execTuples-seams`); the deformed `tts_values`/
//!   `tts_isnull` arrays come back from that call;
//! * the catalog type-output lookups — `getBaseTypeAndTypmod` /
//!   `getTypeOutputInfo` / `getTypeBinaryOutputInfo` (lsyscache.c, via
//!   `backend-utils-cache-lsyscache-seams`);
//! * the fmgr calling convention — `fmgr_info` / `OutputFunctionCall` /
//!   `SendFunctionCall` / `OidOutputFunctionCall` (fmgr.c, via
//!   `backend-utils-fmgr-fmgr-seams`).
//!
//! The Portal's `formats[]` array and target list are *caller-supplied data*
//! (the receiver holds a `Portal` pointer in C; `SetRemoteDestReceiverParams`
//! installs it and the lifecycle hooks read `portal->formats` /
//! `FetchPortalTargetList(portal)`). They are passed in as parameters, not
//! seams. The per-row `tmpcontext` (C's `AllocSetContextCreate` + reset) is the
//! caller's `Mcx` discipline.

#![forbid(unsafe_code)]
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

use ::mcx::Mcx;
use ::types_core::{FmgrInfo, Oid};
use ::types_dest::dest::CommandDest;
use ::types_error::{PgResult, ERRCODE_INVALID_PARAMETER_VALUE, PgError};
use ::nodes::tuptable::SlotData;
use ::stringinfo::StringInfo;
use ::types_tuple::heaptuple::{FormData_pg_attribute, TupleDescData};

use ::pqformat::{
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
/// buffer and the per-row `tmpcontext` belong to the caller's `Mcx`; the
/// `Portal` (and its `formats`/target list) is caller-supplied data passed into
/// the lifecycle hooks. What remains is the receiver bookkeeping printtup.c
/// owns directly.
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
/// parser/planner node tree; the caller hands us this projection of the portal
/// target list.
#[derive(Clone, Copy, Debug, Default)]
pub struct TargetEntryInfo {
    /// C: `TargetEntry->resjunk`.
    pub resjunk: bool,
    /// C: `TargetEntry->resorigtbl`.
    pub resorigtbl: Oid,
    /// C: `TargetEntry->resorigcol`.
    pub resorigcol: i16,
}

/// C: `SetRemoteDestReceiverParams(DestReceiver *self, Portal portal)`. The
/// portal is held by the caller (it belongs to `pquery.c`); this records the
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
///
/// `targetlist` is `FetchPortalTargetList(portal)` projected to the fields
/// printtup reads (empty for a NIL list / utility statement); `formats` is
/// `portal->formats` (`None` for the NULL array — Describe on a prepared stmt).
pub fn printtup_startup<'mcx>(
    myState: &DR_printtup,
    mcx: Mcx<'mcx>,
    _operation: i32,
    typeinfo: &TupleDescData,
    targetlist: &[TargetEntryInfo],
    formats: Option<&[i16]>,
) -> PgResult<StringInfo<'mcx>> {
    // Create I/O buffer to be used for all messages. This cannot be inside
    // tmpcontext, since we want to re-use it across rows. (C: initStringInfo.)
    let mut buf = StringInfo::new_in(mcx);

    // (The C code also creates a "printtup" AllocSet tmpcontext for the per-row
    // workspace; that context is the caller's per-row Mcx discipline.)

    // If we are supposed to emit row descriptions, then send the tuple
    // descriptor of the tuples.
    if myState.sendDescrip {
        SendRowDescriptionMessage(&mut buf, typeinfo, targetlist, formats)?;
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
    targetlist: &[TargetEntryInfo],
    formats: Option<&[i16]>,
) -> PgResult<()> {
    let natts = typeinfo.natts;
    let mut tlist_idx = 0usize;

    // tuple descriptor message type
    pq_beginmessage_reuse(buf, PqMsg_RowDescription);
    // # of attrs in tuples
    pq_sendint16(buf, natts as u16)?;

    for i in 0..natts as usize {
        let att: &FormData_pg_attribute = typeinfo.attr(i);
        let resorigtbl: Oid;
        let resorigcol: i16;
        let format: i16;

        // If column is a domain, send the base type and typmod instead. Lookup
        // before sending any ints, for efficiency. C seeds atttypid/atttypmod
        // from the descriptor, then calls getBaseTypeAndTypmod(typid, &typmod)
        // which leaves *typmod unchanged for a non-domain. The lsyscache seam
        // returns the base type's own typtypmod, so for a non-domain (base ==
        // att.atttypid) we must thread the descriptor's atttypmod through;
        // only a real domain resolution replaces it with the base typmod.
        let (base_typid, base_typmod) =
            lsyscache_seams::get_base_type_and_typmod::call(att.atttypid)?;
        let atttypid = base_typid;
        let atttypmod = if base_typid == att.atttypid {
            att.atttypmod
        } else {
            base_typmod
        };

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

    let _ = pq_endmessage_reuse(buf);
    Ok(())
}

/// C: `printtup_prepare_info(DR_printtup *myState, TupleDesc typeinfo, int
/// numAttrs)` — get the lookup info that `printtup()` needs. Rejects format
/// codes other than 0 (text) and 1 (binary) with the C
/// `ERRCODE_INVALID_PARAMETER_VALUE` "unsupported format code: %d" error.
pub fn printtup_prepare_info<'mcx>(
    myState: &mut DR_printtup,
    mcx: Mcx<'mcx>,
    typeinfo: &TupleDescData,
    formats: Option<&[i16]>,
    numAttrs: i32,
) -> PgResult<()> {
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

        let mut thisState = PrinttupAttrInfo {
            format,
            ..PrinttupAttrInfo::default()
        };
        if format == 0 {
            let (typoutput, typisvarlena) =
                lsyscache_seams::get_type_output_info::call(attr.atttypid)?;
            thisState.typoutput = typoutput;
            thisState.typisvarlena = typisvarlena;
            thisState.finfo = fmgr_seams::fmgr_info::call(mcx, typoutput)?;
        } else if format == 1 {
            let (typsend, typisvarlena) =
                lsyscache_seams::get_type_binary_output_info::call(attr.atttypid)?;
            thisState.typsend = typsend;
            thisState.typisvarlena = typisvarlena;
            thisState.finfo = fmgr_seams::fmgr_info::call(mcx, typsend)?;
        } else {
            return Err(PgError::error(format!("unsupported format code: {format}"))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
        }
        info.push(thisState);
    }
    myState.myinfo = info;
    Ok(())
}

/// C: `printtup(TupleTableSlot *slot, DestReceiver *self)` — send a tuple to
/// the client.
///
/// The caller owns the reusable per-message `buf` (created in
/// [`printtup_startup`]) and the slot. We re-derive attr info if the slot's
/// `TupleDesc` changed, fully deconstruct the tuple (via the execTuples
/// `slot_getallattrs` seam, which returns the deformed `(value, isnull)`
/// columns), then build and flush the DataRow bytes. The per-row `tmpcontext`
/// reset is the caller's `Mcx` discipline.
pub fn printtup<'mcx>(
    myState: &mut DR_printtup,
    mcx: Mcx<'mcx>,
    buf: &mut StringInfo<'mcx>,
    slot: &mut SlotData<'mcx>,
    typeinfo: &TupleDescData,
    formats: Option<&[i16]>,
) -> PgResult<bool> {
    // Make sure the tuple is fully deconstructed. (C: slot_getallattrs(slot);
    // then reads slot->tts_values[i] / slot->tts_isnull[i].) The seam returns
    // the deformed per-attribute (value, isnull) columns directly. Done before
    // `printtup_emit` so the slot's mutable borrow does not overlap a borrow of
    // the slot's own descriptor in the router callback; functionally identical
    // to the C order (prepare_info reads only `typeinfo`/`formats`, never the
    // deformed values).
    let columns = execTuples_seams::slot_getallattrs::call(mcx, slot)?;
    printtup_emit(myState, mcx, buf, typeinfo, formats, &columns)
}

/// The prepare-info + DataRow emit half of C's `printtup`, taking the
/// already-deformed per-attribute `(value, isnull)` columns. Split out from
/// [`printtup`] so the dest-router `receiveSlot` callback can deform the slot
/// (mutable borrow) and then read the slot's descriptor (immutable borrow)
/// without overlapping borrows.
pub fn printtup_emit<'mcx>(
    myState: &mut DR_printtup,
    mcx: Mcx<'mcx>,
    buf: &mut StringInfo<'mcx>,
    typeinfo: &TupleDescData,
    formats: Option<&[i16]>,
    columns: &[::types_tuple::heaptuple::DeformedColumn<'mcx>],
) -> PgResult<bool> {
    let natts = typeinfo.natts;

    // Set or update my derived attribute info, if needed.
    if !myState.attrinfo_matches(typeinfo) || myState.nattrs != natts {
        printtup_prepare_info(myState, mcx, typeinfo, formats, natts)?;
    }

    // Prepare a DataRow message (note buffer is in per-query context).
    pq_beginmessage_reuse(buf, PqMsg_DataRow);
    pq_sendint16(buf, natts as u16)?;

    // send the attributes of this tuple
    for i in 0..natts as usize {
        let (attr, isnull) = &columns[i];

        if *isnull {
            pq_sendint32(buf, (-1i32) as u32)?;
            continue;
        }

        // (The C code here runs VALGRIND_CHECK_MEM_IS_DEFINED over the varlena
        // datum when thisState->typisvarlena; a memory-debugging assert with no
        // functional effect and no analogue under safe Rust.)

        let format = myState.myinfo[i].format;
        if format == 0 {
            // Text output
            let finfo = &myState.myinfo[i].finfo;
            let outputstr = fmgr_seams::output_function_call::call(mcx, finfo, attr)?;
            pq_sendcountedtext(buf, &outputstr)?;
        } else {
            // Binary output
            let finfo = &myState.myinfo[i].finfo;
            let outputbytes = fmgr_seams::send_function_call::call(mcx, finfo, attr)?;
            pq_sendint32(buf, outputbytes.len() as u32)?;
            pq_sendbytes(buf, &outputbytes)?;
        }
    }

    let _ = pq_endmessage_reuse(buf);

    // Return to caller's context, and flush row's temporary memory
    // (C: MemoryContextSwitchTo(oldcontext); MemoryContextReset(tmpcontext)),
    // which is the caller's per-row Mcx discipline.

    Ok(true)
}

/// C: `printtup_shutdown(DestReceiver *self)`. Frees the cached attr info and
/// the receiver bookkeeping (C: `myState->attrinfo = NULL`); the `buf` and
/// `tmpcontext` are the caller's `Mcx`.
pub fn printtup_shutdown(myState: &mut DR_printtup) {
    myState.myinfo.clear();
    myState.nattrs = 0;
    myState.attrinfo = None;
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
pub fn debugStartup(typeinfo: &TupleDescData) -> String {
    let natts = typeinfo.natts;
    let mut out = String::new();
    // show the return type of the tuples
    for i in 0..natts as usize {
        let att: &FormData_pg_attribute = typeinfo.attr(i);
        out.push_str(&printatt((i as u32) + 1, att, None));
    }
    out.push_str("\t----\n");
    out
}

/// C: `debugtup(TupleTableSlot *slot, DestReceiver *self)` — print one tuple
/// for an interactive backend. Returns the accumulated `printf` output and
/// `true` (matching the C `return true`).
pub fn debugtup<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut SlotData<'mcx>,
    typeinfo: &TupleDescData,
) -> PgResult<(String, bool)> {
    // debugtup reads one attr at a time via slot_getattr; deconstruct the slot
    // so the per-attribute (value, isnull) columns are valid.
    let columns = execTuples_seams::slot_getallattrs::call(mcx, slot)?;
    debugtup_emit(mcx, typeinfo, &columns)
}

/// The print half of C's `debugtup`, taking the already-deformed per-attribute
/// `(value, isnull)` columns. Split out from [`debugtup`] so the dest-router
/// `receiveSlot` callback can deform the slot (mutable borrow) and then read its
/// descriptor (immutable borrow) without overlapping borrows — the same split
/// [`printtup_emit`] uses.
pub fn debugtup_emit<'mcx>(
    mcx: Mcx<'mcx>,
    typeinfo: &TupleDescData,
    columns: &[::types_tuple::heaptuple::DeformedColumn<'mcx>],
) -> PgResult<(String, bool)> {
    let natts = typeinfo.natts;
    let mut out = String::new();
    for i in 0..natts as usize {
        let (attr, isnull) = &columns[i];
        if *isnull {
            continue;
        }
        let att: &FormData_pg_attribute = typeinfo.attr(i);
        // getTypeOutputInfo(att->atttypid, &typoutput, &typisvarlena)
        let (typoutput, _typisvarlena) =
            lsyscache_seams::get_type_output_info::call(att.atttypid)?;
        // value = OidOutputFunctionCall(typoutput, attr);
        let value = fmgr_seams::oid_output_function_call::call(mcx, typoutput, attr)?;
        let rendered = Latin1Lossy(&value);
        out.push_str(&printatt((i as u32) + 1, att, Some(&format!("{rendered}"))));
    }
    out.push_str("\t----\n");
    Ok((out, true))
}

// ===========================================================================
// DestReceiver router wiring (printtup.c's DR_printtup, routed into tcop-dest)
// ===========================================================================
//
// `tcop/dest.c`'s `CreateDestReceiver` switch builds the receiver for the
// `DestRemote` / `DestRemoteExecute` / `DestDebug` kinds by calling
// `printtup_create_DR` — which in C installs `printtup` as `receiveSlot` plus
// the `printtup_startup` / `printtup_shutdown` lifecycle hooks on a fresh
// `DR_printtup` struct, and returns the receiver. The owned model carries a
// receiver behind a [`DestReceiverHandle`] in the *one* tcop-dest router; the
// per-receiver `DR_printtup` state (the C `(DR_printtup *) self`) lives here in
// a `thread_local` registry keyed by the router's `state` token, mirroring
// copyto's `CreateCopyDestReceiver` exactly.
//
// The vtable callbacks get `(mcx, state, …)`: `mcx` is the per-query arena
// threaded per dispatch (the message I/O buffer is created in it), `state` is
// the registry index recovering this receiver's `DR_printtup` + the portal's
// `targetlist`/`formats` (bound by `set_remote_dest_receiver_params`).
//
// C reuses one `StringInfo buf` across rows purely for speed (the pre-reserve
// step is an explicit optimisation — see the module docs); the owned model
// charges a fresh `StringInfo` to the threaded per-query `mcx` per message,
// which is functionally identical and keeps the receiver state lifetime-free
// (no `'mcx` smuggled into the `'static` registry, no `unsafe`).

use core::cell::RefCell;
use ::nodes::nodes::CmdType;
use ::nodes::parsestmt::DestReceiverHandle;

/// One printtup receiver's owned state: the `DR_printtup` bookkeeping plus the
/// portal-supplied target-list projection and result formats. This is the
/// owned-model body of C's `DR_printtup` (the `(DR_printtup *) self` the
/// lifecycle hooks downcast to), held in the `RECEIVERS` registry and recovered
/// by the router's `state` token.
struct ReceiverState {
    dr: DR_printtup,
    /// `FetchPortalTargetList(portal)` projected to printtup's fields
    /// (empty for a NIL list / utility statement). Bound by
    /// `set_remote_dest_receiver_params`.
    targetlist: Vec<TargetEntryInfo>,
    /// `portal->formats` (`None` = the C NULL array — Describe on a prepared
    /// stmt). Bound by `set_remote_dest_receiver_params`.
    formats: Option<Vec<i16>>,
}

/// printtup's per-receiver state registry plus a free-list of reclaimed slot
/// indices. A `DR_printtup` is created and destroyed per statement (every
/// `SELECT` to a wire client), so the registry must reuse freed slots instead of
/// growing without bound — otherwise `receiver_register`'s slot search becomes an
/// O(n) scan of a monotonically-growing table on every statement (mirroring C,
/// where each `DR_printtup` is `palloc`'d in the portal context and `pfree`'d on
/// teardown, never accumulating). The free-list makes both register (pop) and
/// unregister (push) O(1).
struct Receivers {
    slots: Vec<Option<ReceiverState>>,
    free: Vec<u32>,
}

impl Receivers {
    const fn new() -> Self {
        Self {
            slots: Vec::new(),
            free: Vec::new(),
        }
    }
}

thread_local! {
    static RECEIVERS: RefCell<Receivers> = const { RefCell::new(Receivers::new()) };
}

/// Allocate a fresh receiver slot holding a new `DR_printtup` for `dest`,
/// returning its 1-based registry index (the router `state` token; 0 is never
/// handed out, matching copyto's convention and the C NULL sentinel). Reuses a
/// freed slot (O(1) free-list pop) before growing the table.
fn receiver_register(dest: CommandDest) -> u64 {
    RECEIVERS.with(|r| {
        let mut reg = r.borrow_mut();
        let st = ReceiverState {
            dr: DR_printtup::printtup_create_DR(dest),
            targetlist: Vec::new(),
            formats: None,
        };
        if let Some(i) = reg.free.pop() {
            reg.slots[i as usize] = Some(st);
            (i + 1) as u64
        } else {
            reg.slots.push(Some(st));
            reg.slots.len() as u64
        }
    })
}

/// Release the receiver state slot named by `state` (the router token; the C
/// `pfree(self)` in `printtup_destroy`), returning its index to the free-list.
/// Idempotent: freeing an already-released or out-of-range token is a no-op.
fn receiver_unregister(state: u64) {
    if state == 0 {
        return;
    }
    RECEIVERS.with(|r| {
        let mut reg = r.borrow_mut();
        let i = (state - 1) as usize;
        if let Some(slot) = reg.slots.get_mut(i) {
            if slot.is_some() {
                *slot = None;
                reg.free.push(i as u32);
            }
        }
    });
}

/// Run `f` against the live `ReceiverState` for `state` (the router token).
fn with_receiver<R>(state: u64, f: impl FnOnce(&mut ReceiverState) -> R) -> R {
    RECEIVERS.with(|r| {
        let mut reg = r.borrow_mut();
        let slot = reg
            .slots
            .get_mut((state - 1) as usize)
            .and_then(Option::as_mut)
            .expect("backend-access-common-printtup: dispatch on an unregistered receiver");
        f(slot)
    })
}

/// `printtup_create_DR(CommandDest dest)` (printtup.c:81) routed into the
/// tcop-dest router: allocate the `DR_printtup` state and install the
/// `printtup_startup` / `printtup` / `printtup_shutdown` callbacks as the
/// receiver's vtable, returning the [`DestReceiverHandle`] that names it.
/// `tcop/dest.c`'s `CreateDestReceiver` reaches this through the
/// `printtup_create_dr` seam, exactly as it reaches copyto's
/// `CreateCopyDestReceiver`.
pub fn printtup_create_dr_routed(dest: CommandDest) -> DestReceiverHandle {
    let state = receiver_register(dest);
    tcop_dest::register_dest_receiver(
        dest,
        tcop_dest::ReceiverVtable {
            rStartup: printtup_dest_startup,
            receiveSlot: printtup_dest_receive,
            rShutdown: printtup_dest_shutdown,
        },
        state,
    )
}

/// `SetRemoteDestReceiverParams(DestReceiver *self, Portal portal)`
/// (printtup.c:121) routed into the dest router: record the portal's result
/// `formats` and the `FetchPortalTargetList(portal)` projection on this
/// receiver's state so the lifecycle hooks can read them, asserting the same
/// `mydest` precondition the C code does. `exec_simple_query` calls this for
/// `DestRemote` after `CreateDestReceiver`.
///
/// The target list is the primary (canSetTag) statement's plan target list
/// (`FetchPortalTargetList` → `PortalGetPrimaryStmt` → `planTree->targetlist`);
/// it is NIL when the portal returns no tuples or is a utility statement, in
/// which case `SendRowDescriptionMessage` sends zeroes for `resorigtbl` /
/// `resorigcol`.
pub fn set_remote_dest_receiver_params_routed(
    receiver: DestReceiverHandle,
    portal: &portal::Portal,
) -> PgResult<()> {
    let state = tcop_dest::dest_receiver_state_token(receiver);

    // C: Assert(myState->mydest == DestRemote || ... == DestRemoteExecute).
    // (DestDebug uses debugStartup/debugtup and never reaches here.)
    with_receiver(state, |st| {
        SetRemoteDestReceiverParams(&st.dr);
    });

    // portal->formats (the int16 array; empty Vec is the C NULL array).
    // FetchPortalTargetList(portal): the primary statement's plan target list,
    // projected to the fields printtup reads.
    let (targetlist, formats) = {
        let p = portal.borrow();
        let formats: Option<Vec<i16>> = if p.formats.is_empty() {
            None
        } else {
            Some(p.formats.clone())
        };
        let targetlist = portal_target_list_info(&p);
        (targetlist, formats)
    };

    with_receiver(state, |st| {
        st.targetlist = targetlist;
        st.formats = formats;
    });
    Ok(())
}

/// `FetchPortalTargetList(portal)` (pquery.c:327) projected to printtup's
/// fields. Returns the primary (canSetTag) statement's plan target-list
/// entries' `(resjunk, resorigtbl, resorigcol)` triples, or an empty Vec for a
/// NIL list (utility statement / non-tuple-returning portal).
fn portal_target_list_info(p: &portal::PortalData) -> Vec<TargetEntryInfo> {
    let stmts = match p.stmts.as_ref() {
        Some(s) => s,
        None => return Vec::new(),
    };
    // PortalGetPrimaryStmt: the first canSetTag statement.
    let primary = match stmts.iter().find(|s| s.canSetTag) {
        Some(s) => s,
        None => return Vec::new(),
    };
    let tlist = match primary.planTree.as_deref() {
        Some(node) => match node.plan_head().targetlist.as_ref() {
            Some(t) => t,
            None => return Vec::new(),
        },
        None => return Vec::new(),
    };
    tlist
        .iter()
        .map(|tle| TargetEntryInfo {
            resjunk: tle.resjunk,
            resorigtbl: tle.resorigtbl,
            resorigcol: tle.resorigcol as i16,
        })
        .collect()
}

/// The dest-router `rStartup` slot for `DR_printtup` — C's `printtup_startup`
/// reached through the router. Builds the RowDescription message in the threaded
/// `mcx` (the I/O buffer) and emits it when `sendDescrip`.
fn printtup_dest_startup<'mcx>(
    mcx: Mcx<'mcx>,
    state: u64,
    operation: CmdType,
    typeinfo: &TupleDescData<'mcx>,
) -> PgResult<()> {
    with_receiver(state, |st| {
        // C: printtup_startup(self, operation, typeinfo) creates the reusable
        // I/O buffer and sends RowDescription when sendDescrip. The buffer is
        // returned (and discarded) here — a fresh StringInfo is charged per
        // message, see the section docs.
        let _buf = printtup_startup(
            &st.dr,
            mcx,
            operation as i32,
            typeinfo,
            &st.targetlist,
            st.formats.as_deref(),
        )?;
        Ok(())
    })
}

/// The dest-router `receiveSlot` slot for `DR_printtup` — C's `printtup`
/// reached through the router. Re-derives attr info if the descriptor changed,
/// deforms the slot, and emits the DataRow message in a fresh `mcx` buffer.
fn printtup_dest_receive<'mcx>(
    mcx: Mcx<'mcx>,
    state: u64,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    // C: printtup(slot, self). Deform the slot first (mutable borrow), then read
    // the slot's descriptor (immutable borrow) for `printtup_emit` — the same
    // split the `printtup`/`printtup_emit` refactor enforces. The descriptor
    // identity caching in DR_printtup still drives the once-per-run
    // re-derivation (the slot's descriptor pointer is stable across a run).
    let columns = execTuples_seams::slot_getallattrs::call(mcx, slot)?;
    let typeinfo = slot
        .base()
        .tts_tupleDescriptor
        .as_deref()
        .expect("printtup: slot has no tuple descriptor");
    with_receiver(state, |st| {
        let mut buf = StringInfo::new_in(mcx);
        let formats = st.formats.clone();
        printtup_emit(
            &mut st.dr,
            mcx,
            &mut buf,
            typeinfo,
            formats.as_deref(),
            &columns,
        )
    })
}

/// The dest-router `rShutdown` slot for `DR_printtup` — C's `printtup_shutdown`
/// reached through the router. Frees the cached attr info / receiver
/// bookkeeping.
fn printtup_dest_shutdown<'mcx>(_mcx: Mcx<'mcx>, state: u64) -> PgResult<()> {
    with_receiver(state, |st| {
        printtup_shutdown(&mut st.dr);
    });
    Ok(())
}

// ===========================================================================
// debugtup DestReceiver router wiring (printtup.c's debugtup/debugStartup, the
// static `debugtupDR` in dest.c, routed into tcop-dest)
// ===========================================================================
//
// `tcop/dest.c`'s `CreateDestReceiver` returns the static
// `debugtupDR = { debugtup, debugStartup, donothingCleanup, donothingCleanup,
// DestDebug }` for `DestDebug`. The standalone (`--single`) backend's
// `whereToSendOutput = DestDebug` (postgres.c:91) routes `SELECT` output here:
// `debugStartup` prints the result column types (one `printatt(.., NULL)` per
// attr) and `debugtup` prints each tuple's columns (`printatt(.., value)`),
// both to **stdout** via C's `printf`.
//
// The C `debugtupDR` carries no per-receiver state (all four slots are static
// functions, `donothingCleanup` for shutdown/destroy), so the owned receiver is
// registered with the stateless `state = 0` token (mirroring `donothingDR`). The
// vtable callbacks below print the strings `debugStartup` / `debugtup` build to
// stdout, exactly as the C `printf` does.

/// Write `s` to stdout, mirroring C's `printf("...")` in `printatt`. The
/// standalone backend's tuples land on the process stdout stream.
fn print_to_stdout(s: &str) {
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut h = stdout.lock();
    // Best-effort like C printf (which ignores the return); a closed stdout in
    // the standalone backend is not an ereport condition.
    let _ = h.write_all(s.as_bytes());
    let _ = h.flush();
}

/// `&debugtupDR` (dest.c:75) routed into the tcop-dest router: register the
/// static debugtup receiver (callbacks `debugStartup` / `debugtup` /
/// `donothingCleanup`) and return the [`DestReceiverHandle`] that names it.
/// `tcop/dest.c`'s `CreateDestReceiver` reaches this through the
/// `create_debug_dest_receiver` seam for `DestDebug`.
pub fn create_debug_dest_receiver_routed() -> DestReceiverHandle {
    tcop_dest::register_dest_receiver(
        CommandDest::Debug,
        tcop_dest::ReceiverVtable {
            rStartup: debugtup_dest_startup,
            receiveSlot: debugtup_dest_receive,
            rShutdown: debugtup_dest_shutdown,
        },
        // The static debugtupDR carries no per-receiver state.
        0,
    )
}

/// The dest-router `rStartup` slot for `debugtupDR` — C's `debugStartup` reached
/// through the router. Prints the result column types to stdout. Carries no
/// state (the `_state` token is the `donothingDR`-style `0`).
fn debugtup_dest_startup<'mcx>(
    _mcx: Mcx<'mcx>,
    _state: u64,
    _operation: CmdType,
    typeinfo: &TupleDescData<'mcx>,
) -> PgResult<()> {
    let out = debugStartup(typeinfo);
    print_to_stdout(&out);
    Ok(())
}

/// The dest-router `receiveSlot` slot for `debugtupDR` — C's `debugtup` reached
/// through the router. Prints one tuple's columns to stdout and returns `true`.
fn debugtup_dest_receive<'mcx>(
    mcx: Mcx<'mcx>,
    _state: u64,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    // C: debugtup(slot, self). Deform the slot first (mutable borrow), then read
    // its descriptor (immutable borrow) — the same split printtup uses.
    let columns = execTuples_seams::slot_getallattrs::call(mcx, slot)?;
    let typeinfo = slot
        .base()
        .tts_tupleDescriptor
        .as_deref()
        .expect("debugtup: slot has no tuple descriptor");
    let (out, ret) = debugtup_emit(mcx, typeinfo, &columns)?;
    print_to_stdout(&out);
    Ok(ret)
}

/// The dest-router `rShutdown` slot for `debugtupDR` — C's `donothingCleanup`.
fn debugtup_dest_shutdown<'mcx>(_mcx: Mcx<'mcx>, _state: u64) -> PgResult<()> {
    Ok(())
}

/// `PqMsg_NoData` (`libpq/protocol.h`).
pub const PqMsg_NoData: u8 = b'n';
/// `PqMsg_ParameterDescription` (`libpq/protocol.h`).
pub const PqMsg_ParameterDescription: u8 = b't';

/// Project a `Node`-wrapped target-list (e.g. the `CachedPlanGetTargetList`
/// result, whose elements are `TargetEntry` nodes) to the printtup-relevant
/// `TargetEntryInfo` triples `SendRowDescriptionMessage` reads.
fn targetlist_info_from_nodes(tlist: &[::nodes::nodes::Node<'_>]) -> Vec<TargetEntryInfo> {
    tlist
        .iter()
        .map(|node| {
            let te = node.expect_targetentry();
            TargetEntryInfo {
                resjunk: te.resjunk,
                resorigtbl: te.resorigtbl,
                resorigcol: te.resorigcol as i16,
            }
        })
        .collect()
}

/// `exec_describe_portal_message`'s reply (postgres.c:2734): send a
/// `RowDescription` describing the portal's result (when `portal->tupDesc`),
/// else a `NoData` message. The target-list projection (`FetchPortalTargetList`)
/// and the per-column result formats are read off the portal here.
///
/// The `whereToSendOutput != DestRemote` early-return and the aborted-xact
/// guard live in the caller (postgres.c); this runs only when there is output
/// to produce.
pub fn send_describe_portal<'mcx>(
    mcx: Mcx<'mcx>,
    portal: &portal::Portal,
) -> PgResult<()> {
    let p = portal.borrow();
    match p.tupDesc.as_ref() {
        Some(tupdesc) => {
            let targetlist = portal_target_list_info(&p);
            let formats: Option<Vec<i16>> = if p.formats.is_empty() {
                None
            } else {
                Some(p.formats.clone())
            };
            // SendRowDescriptionMessage(&row_description_buf, portal->tupDesc,
            //                           FetchPortalTargetList(portal),
            //                           portal->formats);
            let mut buf = ::pqformat::pq_beginmessage(mcx, PqMsg_RowDescription)?;
            // SendRowDescriptionMessage re-begins the message on the same buffer
            // (pq_beginmessage_reuse) and ends it (pq_endmessage_reuse).
            SendRowDescriptionMessage(&mut buf, tupdesc, &targetlist, formats.as_deref())?;
            Ok(())
        }
        None => ::pqformat::pq_putemptymessage(PqMsg_NoData),
    }
}

/// `exec_describe_statement_message`'s reply (postgres.c:2641): first a
/// `ParameterDescription` listing the parameter type OIDs, then a
/// `RowDescription` (when the cached plan has a result descriptor) or a
/// `NoData` message. The plancache reads are threaded in by the caller; the
/// wire encoding + `TargetEntryInfo` projection live here.
///
/// The `whereToSendOutput != DestRemote` early-return and the aborted-xact
/// guard live in the caller (postgres.c).
pub fn send_describe_statement<'mcx>(
    mcx: Mcx<'mcx>,
    param_types: &[Oid],
    result_desc: Option<&TupleDescData<'mcx>>,
    targetlist: &[::nodes::nodes::Node<'mcx>],
) -> PgResult<()> {
    // First describe the parameters...
    //   pq_beginmessage_reuse(&row_description_buf, PqMsg_ParameterDescription);
    //   pq_sendint16(&row_description_buf, psrc->num_params);
    //   for (i = 0; i < psrc->num_params; i++)
    //       pq_sendint32(&row_description_buf, (int) psrc->param_types[i]);
    //   pq_endmessage_reuse(&row_description_buf);
    let mut buf = ::pqformat::pq_beginmessage(mcx, PqMsg_ParameterDescription)?;
    pq_sendint16(&mut buf, param_types.len() as u16)?;
    for &ptype in param_types {
        pq_sendint32(&mut buf, ptype)?;
    }
    pq_endmessage_reuse(&buf)?;

    // Next send RowDescription or NoData to describe the result...
    match result_desc {
        Some(tupdesc) => {
            let tlist = targetlist_info_from_nodes(targetlist);
            // SendRowDescriptionMessage(&row_description_buf, psrc->resultDesc,
            //                           tlist, NULL);
            let mut buf = ::pqformat::pq_beginmessage(mcx, PqMsg_RowDescription)?;
            SendRowDescriptionMessage(&mut buf, tupdesc, &tlist, None)?;
            Ok(())
        }
        None => ::pqformat::pq_putemptymessage(PqMsg_NoData),
    }
}

// ===========================================================================
// EXPLAIN (SERIALIZE) DestReceiver (explain_dr.c) — `SerializeDestReceiver`.
//
// A DestReceiver that serializes passed rows into RowData messages while
// measuring the total serialized size, but never sends the data to the client.
// This exercises deTOASTing and datatype out/sendfuncs without hitting the
// network. The serialization machinery is identical to printtup's (the C
// comment says serializeAnalyzeReceive "should match printtup() as closely as
// possible"), so it lives here alongside printtup rather than in a new crate.
// ===========================================================================

use ::types_core::instrument::SerializeMetrics;

/// `SerializeDestReceiver` (explain_dr.c) private state. The C `DestReceiver
/// pub` head is the router registration; the fields below are the rest of the
/// struct. `es` is replaced by the two flags the receiver actually consults
/// (`es->timing`, `es->buffers`) plus the resolved wire `format`, captured at
/// construction. `attrinfo`/`nattrs`/`finfos` cache the per-column output fn
/// lookup, re-derived when the descriptor changes (as in `serialize_prepare_info`).
struct SerializeState {
    /// The router handle naming this receiver (the C `DestReceiver *`).
    dr_handle: DestReceiverHandle,
    /// `int8 format` — 0 = wire text, 1 = wire binary.
    format: i16,
    /// `es->timing` — measure per-row time. (Timing is not accumulated in this
    /// port; the metric stays zero, which the EXPLAIN regression masks to `N`.)
    timing: bool,
    /// `es->buffers` — accumulate buffer usage. (Likewise not accumulated;
    /// the metric stays zero, masked to `N`.)
    buffers: bool,
    /// `TupleDesc attrinfo` identity + `int nattrs` — cache validity key.
    attrinfo: Option<*const TupleDescData<'static>>,
    nattrs: i32,
    /// `FmgrInfo *finfos` — precomputed output/send call info, one per column.
    finfos: Vec<FmgrInfo>,
    /// `SerializeMetrics metrics` — the collected metrics, read back by
    /// `GetSerializationMetrics`.
    metrics: SerializeMetrics,
}

impl SerializeState {
    fn new(format: i16, timing: bool, buffers: bool) -> Self {
        SerializeState {
            dr_handle: DestReceiverHandle::NULL,
            format,
            timing,
            buffers,
            attrinfo: None,
            nattrs: 0,
            finfos: Vec::new(),
            metrics: SerializeMetrics::default(),
        }
    }
}

thread_local! {
    static SERIALIZE_RECEIVERS: RefCell<Vec<Option<SerializeState>>> =
        const { RefCell::new(Vec::new()) };
}

fn serialize_register(state: SerializeState) -> u64 {
    SERIALIZE_RECEIVERS.with(|r| {
        let mut reg = r.borrow_mut();
        if let Some(i) = reg.iter().position(Option::is_none) {
            reg[i] = Some(state);
            (i + 1) as u64
        } else {
            reg.push(Some(state));
            reg.len() as u64
        }
    })
}

fn with_serialize<R>(token: u64, f: impl FnOnce(&mut SerializeState) -> R) -> R {
    SERIALIZE_RECEIVERS.with(|r| {
        let mut reg = r.borrow_mut();
        let slot = reg
            .get_mut((token - 1) as usize)
            .and_then(Option::as_mut)
            .expect("backend-access-common-printtup: dispatch on an unregistered serialize receiver");
        f(slot)
    })
}

/// `serialize_prepare_info(receiver, typeinfo, nattrs)` (explain_dr.c) — get the
/// function lookup info we'll need for output. A subset of
/// `printtup_prepare_info` (no per-column format choices: one format for all).
fn serialize_prepare_info<'mcx>(
    st: &mut SerializeState,
    mcx: Mcx<'mcx>,
    typeinfo: &TupleDescData<'mcx>,
    nattrs: i32,
) -> PgResult<()> {
    st.finfos.clear();
    st.attrinfo = Some(typeinfo as *const _ as *const TupleDescData<'static>);
    st.nattrs = nattrs;
    if nattrs <= 0 {
        return Ok(());
    }
    st.finfos
        .try_reserve(nattrs as usize)
        .map_err(|_| PgError::error("serialize_prepare_info: out of memory"))?;
    for i in 0..nattrs as usize {
        let attr = typeinfo.attr(i);
        let finfo = if st.format == 0 {
            // wire protocol format text
            let (typoutput, _typisvarlena) =
                lsyscache_seams::get_type_output_info::call(attr.atttypid)?;
            fmgr_seams::fmgr_info::call(mcx, typoutput)?
        } else if st.format == 1 {
            // wire protocol format binary
            let (typsend, _typisvarlena) =
                lsyscache_seams::get_type_binary_output_info::call(
                    attr.atttypid,
                )?;
            fmgr_seams::fmgr_info::call(mcx, typsend)?
        } else {
            return Err(PgError::error(format!("unsupported format code: {}", st.format))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
        };
        st.finfos.push(finfo);
    }
    Ok(())
}

/// `serializeAnalyzeStartup(self, operation, typeinfo)` (explain_dr.c) — start
/// up the serialize receiver. Format was resolved at construction; reset the
/// metrics.
fn serialize_startup<'mcx>(
    _mcx: Mcx<'mcx>,
    state: u64,
    _operation: CmdType,
    _typeinfo: &TupleDescData<'mcx>,
) -> PgResult<()> {
    with_serialize(state, |st| {
        st.metrics = SerializeMetrics::default();
    });
    Ok(())
}

/// `serializeAnalyzeReceive(slot, self)` (explain_dr.c) — serialize one tuple,
/// counting the bytes that would have been sent. Matches `printtup` except the
/// constructed `DataRow` buffer is counted, not flushed to the client.
fn serialize_receive<'mcx>(
    mcx: Mcx<'mcx>,
    state: u64,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    // Make sure the tuple is fully deconstructed (C: slot_getallattrs(slot)).
    let columns = execTuples_seams::slot_getallattrs::call(mcx, slot)?;
    let typeinfo = slot
        .base()
        .tts_tupleDescriptor
        .as_deref()
        .expect("serialize receiver: slot has no tuple descriptor");
    let natts = typeinfo.natts;

    with_serialize(state, |st| {
        // Set or update derived attribute info, if needed.
        let cache_valid = st
            .attrinfo
            .map(|p| core::ptr::eq(p as *const TupleDescData, typeinfo as *const _))
            .unwrap_or(false);
        if !cache_valid || st.nattrs != natts {
            serialize_prepare_info(st, mcx, typeinfo, natts)?;
        }

        // Prepare a DataRow message (note buffer is in per-row context).
        let mut buf = StringInfo::new_in(mcx);
        pq_beginmessage_reuse(&mut buf, PqMsg_DataRow);
        pq_sendint16(&mut buf, natts as u16)?;

        for i in 0..natts as usize {
            let (attr, isnull) = &columns[i];
            if *isnull {
                pq_sendint32(&mut buf, (-1i32) as u32)?;
                continue;
            }
            if st.format == 0 {
                // Text output
                let outputstr =
                    fmgr_seams::output_function_call::call(mcx, &st.finfos[i], attr)?;
                pq_sendcountedtext(&mut buf, &outputstr)?;
            } else {
                // Binary output
                let outputbytes =
                    fmgr_seams::send_function_call::call(mcx, &st.finfos[i], attr)?;
                pq_sendint32(&mut buf, outputbytes.len() as u32)?;
                pq_sendbytes(&mut buf, &outputbytes)?;
            }
        }

        // We mustn't flush the message (that would send data to the client).
        // Just count the data, exactly as C's `metrics.bytesSent += buf->len`.
        st.metrics.bytesSent += buf.len() as u64;
        Ok::<(), PgError>(())
    })?;

    Ok(true)
}

/// `serializeAnalyzeShutdown(self)` (explain_dr.c) — drop the cached attr info.
fn serialize_shutdown<'mcx>(_mcx: Mcx<'mcx>, state: u64) -> PgResult<()> {
    with_serialize(state, |st| {
        st.finfos.clear();
        st.attrinfo = None;
        st.nattrs = 0;
    });
    Ok(())
}

/// `CreateExplainSerializeDestReceiver(es)` (explain_dr.c) — build the SERIALIZE
/// `DestReceiver` and register it into the tcop-dest router, returning its
/// [`DestReceiverHandle`]. The flags it needs (`es->timing`, `es->buffers`) and
/// the resolved wire `format` (0 = text, 1 = binary) are captured here, in lieu
/// of holding the `ExplainState *`.
pub fn create_explain_serialize_dest_receiver_routed(
    format: i16,
    timing: bool,
    buffers: bool,
) -> DestReceiverHandle {
    let token = serialize_register(SerializeState::new(format, timing, buffers));
    let dr = tcop_dest::register_dest_receiver(
        CommandDest::ExplainSerialize,
        tcop_dest::ReceiverVtable {
            rStartup: serialize_startup,
            receiveSlot: serialize_receive,
            rShutdown: serialize_shutdown,
        },
        token,
    );
    with_serialize(token, |st| st.dr_handle = dr);
    dr
}

/// `GetSerializationMetrics(dest)` (explain_dr.c) — collect metrics. If `dest`
/// is not a SERIALIZE receiver (e.g. an IntoRel receiver for CREATE TABLE AS),
/// return all-zeroes stats.
pub fn get_serialization_metrics_routed(dest: DestReceiverHandle) -> SerializeMetrics {
    if dest == DestReceiverHandle::NULL {
        return SerializeMetrics::default();
    }
    let token = SERIALIZE_RECEIVERS.with(|r| {
        r.borrow()
            .iter()
            .position(|s| matches!(s, Some(st) if st.dr_handle == dest))
            .map(|i| (i + 1) as u64)
    });
    match token {
        Some(t) => with_serialize(t, |st| st.metrics),
        None => SerializeMetrics::default(),
    }
}

/// Install this crate's inward seams (the printtup / debugtup dest-router
/// constructors and `SetRemoteDestReceiverParams`). Wired into `seams-init`.
pub fn init_seams() {
    printtup_seams::create_explain_serialize_dest_receiver::set(
        create_explain_serialize_dest_receiver_routed,
    );
    printtup_seams::get_serialization_metrics::set(
        get_serialization_metrics_routed,
    );
    printtup_seams::printtup_create_dr::set(printtup_create_dr_routed);
    printtup_seams::printtup_free_dr::set(receiver_unregister);
    printtup_seams::send_describe_portal::set(send_describe_portal);
    printtup_seams::send_describe_statement::set(send_describe_statement);
    printtup_seams::create_debug_dest_receiver::set(
        create_debug_dest_receiver_routed,
    );
    printtup_seams::create_remote_simple_dest_receiver::set(
        printsimple::create_remote_simple_dest_receiver_routed,
    );
    dest_seams::set_remote_dest_receiver_params::set(
        set_remote_dest_receiver_params_routed,
    );
}

mod printsimple;

#[cfg(test)]
mod tests;
