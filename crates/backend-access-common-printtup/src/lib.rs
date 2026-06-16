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

use mcx::Mcx;
use types_core::{FmgrInfo, Oid};
use types_dest::dest::CommandDest;
use types_error::{PgResult, ERRCODE_INVALID_PARAMETER_VALUE, PgError};
use types_nodes::tuptable::SlotData;
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
        // before sending any ints, for efficiency. (C seeds atttypid/atttypmod
        // from the descriptor, then overwrites both with getBaseTypeAndTypmod.)
        let (atttypid, atttypmod) =
            backend_utils_cache_lsyscache_seams::get_base_type_and_typmod::call(att.atttypid)?;

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
                backend_utils_cache_lsyscache_seams::get_type_output_info::call(attr.atttypid)?;
            thisState.typoutput = typoutput;
            thisState.typisvarlena = typisvarlena;
            thisState.finfo = backend_utils_fmgr_fmgr_seams::fmgr_info::call(mcx, typoutput)?;
        } else if format == 1 {
            let (typsend, typisvarlena) =
                backend_utils_cache_lsyscache_seams::get_type_binary_output_info::call(attr.atttypid)?;
            thisState.typsend = typsend;
            thisState.typisvarlena = typisvarlena;
            thisState.finfo = backend_utils_fmgr_fmgr_seams::fmgr_info::call(mcx, typsend)?;
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
    let columns = backend_executor_execTuples_seams::slot_getallattrs::call(mcx, slot)?;
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
    columns: &[types_tuple::backend_access_common_heaptuple::DeformedColumn<'mcx>],
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
            let outputstr = backend_utils_fmgr_fmgr_seams::output_function_call::call(mcx, finfo, attr)?;
            pq_sendcountedtext(buf, &outputstr)?;
        } else {
            // Binary output
            let finfo = &myState.myinfo[i].finfo;
            let outputbytes = backend_utils_fmgr_fmgr_seams::send_function_call::call(mcx, finfo, attr)?;
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
    let natts = typeinfo.natts;
    // debugtup reads one attr at a time via slot_getattr; deconstruct the slot
    // so the per-attribute (value, isnull) columns are valid.
    let columns = backend_executor_execTuples_seams::slot_getallattrs::call(mcx, slot)?;

    let mut out = String::new();
    for i in 0..natts as usize {
        let (attr, isnull) = &columns[i];
        if *isnull {
            continue;
        }
        let att: &FormData_pg_attribute = typeinfo.attr(i);
        // getTypeOutputInfo(att->atttypid, &typoutput, &typisvarlena)
        let (typoutput, _typisvarlena) =
            backend_utils_cache_lsyscache_seams::get_type_output_info::call(att.atttypid)?;
        // value = OidOutputFunctionCall(typoutput, attr);
        let value = backend_utils_fmgr_fmgr_seams::oid_output_function_call::call(mcx, typoutput, attr)?;
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
use types_nodes::nodes::CmdType;
use types_nodes::parsestmt::DestReceiverHandle;

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

thread_local! {
    static RECEIVERS: RefCell<Vec<Option<ReceiverState>>> = const { RefCell::new(Vec::new()) };
}

/// Allocate a fresh receiver slot holding a new `DR_printtup` for `dest`,
/// returning its 1-based registry index (the router `state` token; 0 is never
/// handed out, matching copyto's convention and the C NULL sentinel).
fn receiver_register(dest: CommandDest) -> u64 {
    RECEIVERS.with(|r| {
        let mut reg = r.borrow_mut();
        let st = ReceiverState {
            dr: DR_printtup::printtup_create_DR(dest),
            targetlist: Vec::new(),
            formats: None,
        };
        if let Some(i) = reg.iter().position(Option::is_none) {
            reg[i] = Some(st);
            (i + 1) as u64
        } else {
            reg.push(Some(st));
            reg.len() as u64
        }
    })
}

/// Run `f` against the live `ReceiverState` for `state` (the router token).
fn with_receiver<R>(state: u64, f: impl FnOnce(&mut ReceiverState) -> R) -> R {
    RECEIVERS.with(|r| {
        let mut reg = r.borrow_mut();
        let slot = reg
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
    backend_tcop_dest::register_dest_receiver(
        dest,
        backend_tcop_dest::ReceiverVtable {
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
    portal: &types_portal::Portal,
) -> PgResult<()> {
    let state = backend_tcop_dest::dest_receiver_state_token(receiver);

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
fn portal_target_list_info(p: &types_portal::PortalData) -> Vec<TargetEntryInfo> {
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
    let columns = backend_executor_execTuples_seams::slot_getallattrs::call(mcx, slot)?;
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

/// Install this crate's inward seams (the printtup dest-router constructor and
/// `SetRemoteDestReceiverParams`). Wired into `seams-init`.
pub fn init_seams() {
    backend_access_common_printtup_seams::printtup_create_dr::set(printtup_create_dr_routed);
    backend_tcop_dest_seams::set_remote_dest_receiver_params::set(
        set_remote_dest_receiver_params_routed,
    );
}

#[cfg(test)]
mod tests;
