//! Port of `src/backend/access/common/printsimple.c`.
//!
//! The `DestRemoteSimple` receiver: prints tuples containing only a limited
//! range of builtin types (TEXT / INT4 / INT8 / OID) without catalog access.
//! Used by backends that are not bound to a specific database — notably some
//! walsender processes (`IDENTIFY_SYSTEM`, `READ_REPLICATION_SLOT`,
//! `TIMELINE_HISTORY`, `SHOW`). Only protocol 3.0 is handled, exactly as the C.
//!
//! The receiver is routed into the `tcop-dest` router (the same delegation
//! `printtup_create_DR` / copyto's `CreateCopyDestReceiver` use): the router's
//! `CreateDestReceiver(DestRemoteSimple)` reaches a constructor here through the
//! `create_remote_simple_dest_receiver` seam, which registers the
//! `printsimple_startup` / `printsimple` / (no-op shutdown) vtable.

use mcx::Mcx;
use types_dest::dest::CommandDest;
use types_error::{PgError, PgResult};
use nodes::nodes::CmdType;
use nodes::parsestmt::DestReceiverHandle;
use nodes::tuptable::SlotData;
use stringinfo::StringInfo;
use types_tuple::heaptuple::TupleDescData;

use pqformat::{
    pq_beginmessage_reuse, pq_endmessage_reuse, pq_sendcountedtext, pq_sendint16, pq_sendint32,
};

use crate::{PqMsg_DataRow, PqMsg_RowDescription};

/// `TEXTOID` (catalog/pg_type.dat).
const TEXTOID: types_core::Oid = 25;
/// `INT4OID`.
const INT4OID: types_core::Oid = 23;
/// `INT8OID`.
const INT8OID: types_core::Oid = 20;
/// `OIDOID`.
const OIDOID: types_core::Oid = 26;

/// `void printsimple_startup(DestReceiver *self, int operation, TupleDesc
/// tupdesc)` (printsimple.c:30) — send a RowDescription message.
///
/// `operation` is unused (matching the C), and there is no per-receiver state.
fn printsimple_startup<'mcx>(
    mcx: Mcx<'mcx>,
    _state: u64,
    _operation: CmdType,
    tupdesc: &TupleDescData<'mcx>,
) -> PgResult<()> {
    let mut buf = StringInfo::new_in(mcx);

    // pq_beginmessage(&buf, PqMsg_RowDescription);
    pq_beginmessage_reuse(&mut buf, PqMsg_RowDescription);
    // pq_sendint16(&buf, tupdesc->natts);
    pq_sendint16(&mut buf, tupdesc.natts as u16)?;

    for i in 0..tupdesc.natts as usize {
        let attr = tupdesc.attr(i);

        // pq_sendstring(&buf, NameStr(attr->attname));
        pqformat::pq_sendstring(&mut buf, attr.attname.name_str())?;
        // pq_sendint32(&buf, 0);  /* table oid */
        pq_sendint32(&mut buf, 0)?;
        // pq_sendint16(&buf, 0);  /* attnum */
        pq_sendint16(&mut buf, 0)?;
        // pq_sendint32(&buf, (int) attr->atttypid);
        pq_sendint32(&mut buf, attr.atttypid)?;
        // pq_sendint16(&buf, attr->attlen);
        pq_sendint16(&mut buf, attr.attlen as u16)?;
        // pq_sendint32(&buf, attr->atttypmod);
        pq_sendint32(&mut buf, attr.atttypmod as u32)?;
        // pq_sendint16(&buf, 0);  /* format code */
        pq_sendint16(&mut buf, 0)?;
    }

    // pq_endmessage(&buf);
    let _ = pq_endmessage_reuse(&buf);
    Ok(())
}

/// `bool printsimple(TupleTableSlot *slot, DestReceiver *self)`
/// (printsimple.c:58) — send a DataRow message for one tuple.
fn printsimple_receive<'mcx>(
    mcx: Mcx<'mcx>,
    _state: u64,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    // slot_getallattrs(slot): the seam returns the deformed per-attribute
    // (value, isnull) columns directly. Deform before reading the slot's own
    // descriptor (the same mutable/immutable borrow split printtup uses).
    let columns = execTuples_seams::slot_getallattrs::call(mcx, slot)?;
    let tupdesc = slot
        .base()
        .tts_tupleDescriptor
        .as_deref()
        .expect("printsimple: slot has no tuple descriptor");

    let natts = tupdesc.natts;

    let mut buf = StringInfo::new_in(mcx);
    // pq_beginmessage(&buf, PqMsg_DataRow);
    pq_beginmessage_reuse(&mut buf, PqMsg_DataRow);
    // pq_sendint16(&buf, tupdesc->natts);
    pq_sendint16(&mut buf, natts as u16)?;

    for i in 0..natts as usize {
        let attr = tupdesc.attr(i);
        let (value, isnull) = &columns[i];

        if *isnull {
            // pq_sendint32(&buf, -1);
            pq_sendint32(&mut buf, (-1i32) as u32)?;
            continue;
        }

        // We can't call the regular type output functions here because we might
        // not have catalog access. Hard-wire knowledge of the required types.
        match attr.atttypid {
            TEXTOID => {
                // text *t = DatumGetTextPP(value);
                // pq_sendcountedtext(&buf, VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t));
                let s = varlena_seams::text_to_cstring_v::call(mcx, value)?;
                pq_sendcountedtext(&mut buf, s.as_str().as_bytes())?;
            }
            INT4OID => {
                // int32 num = DatumGetInt32(value); len = pg_ltoa(num, str);
                let num = value.as_i32();
                let s = format!("{num}");
                pq_sendcountedtext(&mut buf, s.as_bytes())?;
            }
            INT8OID => {
                // int64 num = DatumGetInt64(value); len = pg_lltoa(num, str);
                let num = value.as_i64();
                let s = format!("{num}");
                pq_sendcountedtext(&mut buf, s.as_bytes())?;
            }
            OIDOID => {
                // Oid num = ObjectIdGetDatum(value); len = pg_ultoa_n(num, str);
                let num = value.as_u32();
                let s = format!("{num}");
                pq_sendcountedtext(&mut buf, s.as_bytes())?;
            }
            other => {
                // elog(ERROR, "unsupported type OID: %u", attr->atttypid);
                return Err(PgError::error(format!("unsupported type OID: {other}")));
            }
        }
    }

    // pq_endmessage(&buf);
    let _ = pq_endmessage_reuse(&buf);
    Ok(true)
}

/// `donothingCleanup` / `donothingShutdown` (dest.c) — the `printsimpleDR`
/// vtable's `rShutdown` is `donothingShutdown` (printsimple has no per-receiver
/// state to free).
fn printsimple_shutdown<'mcx>(_mcx: Mcx<'mcx>, _state: u64) -> PgResult<()> {
    Ok(())
}

/// `CreateDestReceiver(DestRemoteSimple)` (dest.c) → the static `printsimpleDR`
/// (printsimple.h): register the printsimple vtable into the tcop-dest router
/// and return the handle naming it. Reached through the
/// `create_remote_simple_dest_receiver` seam.
pub fn create_remote_simple_dest_receiver_routed() -> DestReceiverHandle {
    tcop_dest::register_dest_receiver(
        CommandDest::RemoteSimple,
        tcop_dest::ReceiverVtable {
            rStartup: printsimple_startup,
            receiveSlot: printsimple_receive,
            rShutdown: printsimple_shutdown,
        },
        0,
    )
}
