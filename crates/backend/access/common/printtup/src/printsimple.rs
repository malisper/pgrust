// printsimple.c — DestRemoteSimple per-row output without catalog access
// (PG 18.3); the hard-wired type set is TEXTOID/INT4OID/INT8OID/OIDOID.

use ::pqformat::{
    pq_beginmessage_reuse, pq_endmessage_reuse, pq_sendcountedtext, pq_sendint16, pq_sendint32,
    pq_sendstring,
};
use ::stringinfo::StringInfo;
use ::types_core::{INT4OID, INT8OID, OIDOID, TEXTOID};
use ::types_error::{PgError, PgResult};
use ::types_fmgr::PackedVarlena;
use ::types_slot::SlotData;
use ::types_tuple::TupleDescData;

const PQMSG_ROW_DESCRIPTION: u8 = b'T';
const PQMSG_DATA_ROW: u8 = b'D';

pub struct DrPrintsimple {
    buf: Option<StringInfo<'static>>,
}

pub fn printsimple_create_DR() -> DrPrintsimple {
    DrPrintsimple { buf: None }
}

impl DrPrintsimple {
    // printsimple_startup: send a RowDescription message.
    pub fn startup(&mut self, _operation: i32, tupdesc: &TupleDescData<'_>) -> PgResult<()> {
        let mut buf = crate::take_wire_buf()?;

        pq_beginmessage_reuse(&mut buf, PQMSG_ROW_DESCRIPTION);
        pq_sendint16(&mut buf, tupdesc.natts as u16)?;

        for i in 0..tupdesc.natts as usize {
            let attr = tupdesc.attr(i);
            pq_sendstring(&mut buf, attr.attname.name_str())?;
            pq_sendint32(&mut buf, 0)?; // table oid
            pq_sendint16(&mut buf, 0)?; // attnum
            pq_sendint32(&mut buf, attr.atttypid)?;
            pq_sendint16(&mut buf, attr.attlen as u16)?;
            pq_sendint32(&mut buf, attr.atttypmod as u32)?;
            pq_sendint16(&mut buf, 0)?; // format code
        }

        pq_endmessage_reuse(&buf)?;
        self.buf = Some(buf);
        Ok(())
    }

    // printsimple: send a DataRow message per tuple.
    pub fn receive_slot(&mut self, slot: &mut SlotData<'_>) -> PgResult<bool> {
        exectuples::slot_getallattrs(slot);

        let base = slot.base();
        let tupdesc = base
            .tts_tupleDescriptor
            .as_ref()
            .expect("printsimple: slot without descriptor")
            .clone();
        let buf = self
            .buf
            .as_mut()
            .expect("printsimple before printsimple_startup");

        pq_beginmessage_reuse(buf, PQMSG_DATA_ROW);
        pq_sendint16(buf, tupdesc.natts as u16)?;

        for i in 0..tupdesc.natts as usize {
            if base.tts_isnull[i] {
                pq_sendint32(buf, (-1i32) as u32)?;
                continue;
            }
            let value = base.tts_values[i];
            let attr = tupdesc.attr(i);

            // No catalog access here: hard-wired knowledge of the required types.
            match attr.atttypid {
                TEXTOID => {
                    // SAFETY: a TEXTOID datum in a materialized slot is an
                    // inline varlena image (C's DatumGetTextPP contract).
                    let t = unsafe { PackedVarlena::from_ptr(value.as_usize() as *const u8) };
                    pq_sendcountedtext(buf, t.data())?;
                }
                INT4OID => {
                    let mut str = [0u8; 12];
                    let len = numutils::pg_ltoa(value.as_i32(), &mut str);
                    pq_sendcountedtext(buf, &str[..len])?;
                }
                INT8OID => {
                    let mut str = [0u8; 21];
                    let len = numutils::pg_lltoa(value.as_i64(), &mut str);
                    pq_sendcountedtext(buf, &str[..len])?;
                }
                OIDOID => {
                    let mut str = [0u8; 10];
                    let len = numutils::pg_ultoa_n(value.as_u32(), &mut str);
                    pq_sendcountedtext(buf, &str[..len])?;
                }
                other => return Err(unsupported_type(other)),
            }
        }

        pq_endmessage_reuse(buf)?;
        Ok(true)
    }

    pub fn shutdown(&mut self) {
        if let Some(buf) = self.buf.take() {
            crate::put_wire_buf(buf);
        }
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn unsupported_type(oid: ::types_core::Oid) -> Box<PgError> {
    PgError::error(format!("unsupported type OID: {oid}")).into()
}
