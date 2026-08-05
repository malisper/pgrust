//! Port of `basebackup_copy.c`: the COPY-protocol base-backup sink. In-band
//! CopyData type bytes: 'n' archive, 'd' data, 'm' manifest, 'p' progress.

use std::boxed::Box;
use std::string::{String, ToString};
use std::vec;
use std::vec::Vec;

use ::sink::{Bbsink, BbsinkOps, BbsinkState};
use ::pqcomm_seams::pq_putmessage;
use ::pqformat::{
    pq_beginmessage, pq_endmessage, pq_putemptymessage, pq_puttextmessage, pq_sendbyte,
    pq_sendint16, pq_sendint64, pq_sendstring,
};
use ::timestamp_seams::get_current_timestamp;
use ::mcx::Mcx;
use ::types_core::{Size, TimeLineID, TimestampTz, XLogRecPtr};
use ::types_error::PgResult;

use ::backup_copy_seams as seam;
use ::backup_copy_seams::{ResultColumn, ResultColumnType, ResultValue};

const TEXTOID: ResultColumnType = ResultColumnType::Text;
const INT8OID: ResultColumnType = ResultColumnType::Int8;
const OIDOID: ResultColumnType = ResultColumnType::Oid;

const SELECT_TAG: &[u8] = b"SELECT";

pub const PQ_MSG_COMMAND_COMPLETE: u8 = b'C';
pub const PQ_MSG_COPY_DATA: u8 = b'd';
pub const PQ_MSG_COPY_DONE: u8 = b'c';
pub const PQ_MSG_COPY_OUT_RESPONSE: u8 = b'H';

const PROGRESS_REPORT_BYTE_INTERVAL: u64 = 65536;
const PROGRESS_REPORT_MILLISECOND_THRESHOLD: i64 = 1000;

/// C `bbsink_copystream`. The forwarding chain (empty, leaf) and working buffer
/// live in the surrounding [`Bbsink`]; this carries the `send_to_client` flag,
/// the message-assembly `mcx`, the retained data-frame scratch, and the
/// progress-timer state.
pub struct BbsinkCopystream<'mcx> {
    send_to_client: bool,
    mcx: Mcx<'mcx>,
    scratch: Vec<u8>,
    last_progress_report_time: TimestampTz,
    bytes_done_at_last_time_check: u64,
}
pub fn bbsink_copystream_new<'mcx>(mcx: Mcx<'mcx>, send_to_client: bool) -> Box<Bbsink<'mcx>> {
    let ops = BbsinkCopystream {
        send_to_client,
        mcx,
        scratch: Vec::new(),
        last_progress_report_time: get_current_timestamp::call(),
        bytes_done_at_last_time_check: 0,
    };
    Box::new(Bbsink::new(mcx, Box::new(ops), None))
}

impl<'mcx> BbsinkCopystream<'mcx> {
    /// Ship a `CopyData` message whose payload is the in-band `'d'` type byte
    /// followed by `len` bytes of the working buffer, matching the C
    /// `pq_putmessage('d', msgbuffer, len + 1)` single-call framing.
    fn put_data(&mut self, sink: &Bbsink<'mcx>, len: Size) -> PgResult<()> {
        self.scratch.clear();
        self.scratch.push(b'd');
        self.scratch.extend_from_slice(sink.buffer_slice(len));
        let _eof = pq_putmessage::call(PQ_MSG_COPY_DATA, &self.scratch)?;
        Ok(())
    }
}

impl<'mcx> BbsinkOps<'mcx> for BbsinkCopystream<'mcx> {
    fn begin_backup(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        let buffer_length = sink.buffer_length();
        sink.set_buffer(self.mcx, buffer_length)?;
        self.scratch.reserve(buffer_length + 1);

        send_xlog_rec_ptr_result(self.mcx, state.startptr, state.starttli)?;
        send_tablespace_list(state)?;
        pq_puttextmessage(self.mcx, PQ_MSG_COMMAND_COMPLETE, SELECT_TAG)?;
        send_copy_out_response(self.mcx)
    }

    fn begin_archive(
        &mut self,
        _sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        archive_name: &str,
    ) -> PgResult<()> {
        let ti = &state.tablespaces[state.tablespace_num as usize];
        let mut buf = pq_beginmessage(self.mcx, PQ_MSG_COPY_DATA)?;
        pq_sendbyte(&mut buf, b'n')?;
        pq_sendstring(&mut buf, archive_name.as_bytes())?;
        let path = ti.path.as_deref().unwrap_or("");
        pq_sendstring(&mut buf, path.as_bytes())?;
        pq_endmessage(buf)
    }

    fn archive_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        if self.send_to_client {
            self.put_data(sink, len)?;
        }

        // C uint64 addition wraps; use wrapping_add to match.
        let targetbytes = self
            .bytes_done_at_last_time_check
            .wrapping_add(PROGRESS_REPORT_BYTE_INTERVAL);
        if targetbytes <= state.bytes_done {
            let now = get_current_timestamp::call();
            self.bytes_done_at_last_time_check = state.bytes_done;
            let ms = timestamp_difference_milliseconds(self.last_progress_report_time, now);
            if ms >= PROGRESS_REPORT_MILLISECOND_THRESHOLD || now < self.last_progress_report_time {
                self.last_progress_report_time = now;
                let mut buf = pq_beginmessage(self.mcx, PQ_MSG_COPY_DATA)?;
                pq_sendbyte(&mut buf, b'p')?;
                pq_sendint64(&mut buf, state.bytes_done)?;
                pq_endmessage(buf)?;
                let _pending = seam::pq_flush_if_writable::call()?;
            }
        }
        Ok(())
    }

    fn end_archive(&mut self, _sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        self.bytes_done_at_last_time_check = state.bytes_done;
        self.last_progress_report_time = get_current_timestamp::call();
        let mut buf = pq_beginmessage(self.mcx, PQ_MSG_COPY_DATA)?;
        pq_sendbyte(&mut buf, b'p')?;
        pq_sendint64(&mut buf, state.bytes_done)?;
        pq_endmessage(buf)?;
        let _pending = seam::pq_flush_if_writable::call()?;
        Ok(())
    }

    fn begin_manifest(&mut self, _sink: &mut Bbsink<'mcx>, _state: &mut BbsinkState) -> PgResult<()> {
        let mut buf = pq_beginmessage(self.mcx, PQ_MSG_COPY_DATA)?;
        pq_sendbyte(&mut buf, b'm')?;
        pq_endmessage(buf)
    }

    fn manifest_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        _state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        if self.send_to_client {
            self.put_data(sink, len)?;
        }
        Ok(())
    }

    fn end_manifest(&mut self, _sink: &mut Bbsink<'mcx>, _state: &mut BbsinkState) -> PgResult<()> {
        Ok(())
    }

    fn end_backup(
        &mut self,
        _sink: &mut Bbsink<'mcx>,
        _state: &mut BbsinkState,
        endptr: XLogRecPtr,
        endtli: TimeLineID,
    ) -> PgResult<()> {
        send_copy_done()?;
        send_xlog_rec_ptr_result(self.mcx, endptr, endtli)
    }

    fn cleanup(&mut self, _sink: &mut Bbsink<'mcx>, _state: &mut BbsinkState) -> PgResult<()> {
        Ok(())
    }
}

fn send_copy_out_response(mcx: Mcx<'_>) -> PgResult<()> {
    let mut buf = pq_beginmessage(mcx, PQ_MSG_COPY_OUT_RESPONSE)?;
    pq_sendbyte(&mut buf, 0)?;
    pq_sendint16(&mut buf, 0)?;
    pq_endmessage(buf)
}

fn send_copy_done() -> PgResult<()> {
    pq_putemptymessage(PQ_MSG_COPY_DONE)
}

fn send_xlog_rec_ptr_result(mcx: Mcx<'_>, ptr: XLogRecPtr, tli: TimeLineID) -> PgResult<()> {
    let dest = seam::create_dest_remote_simple::call();
    let columns = vec![
        ResultColumn { name: "recptr".to_string(), typ: TEXTOID },
        ResultColumn { name: "tli".to_string(), typ: INT8OID },
    ];
    let tstate = seam::begin_tup_output_tupdesc::call(dest, columns);
    let values = vec![
        Some(ResultValue::Text(format_lsn(ptr))),
        Some(ResultValue::Int8(tli as i64)),
    ];
    seam::do_tup_output::call(tstate, values);
    seam::end_tup_output::call(tstate);
    pq_puttextmessage(mcx, PQ_MSG_COMMAND_COMPLETE, SELECT_TAG)
}

fn send_tablespace_list(state: &BbsinkState) -> PgResult<()> {
    let dest = seam::create_dest_remote_simple::call();
    let columns = vec![
        ResultColumn { name: "spcoid".to_string(), typ: OIDOID },
        ResultColumn { name: "spclocation".to_string(), typ: TEXTOID },
        ResultColumn { name: "size".to_string(), typ: INT8OID },
    ];
    let tstate = seam::begin_tup_output_tupdesc::call(dest, columns);

    for ti in &state.tablespaces {
        let (spcoid, spclocation) = match ti.path.as_deref() {
            Some(path) => (
                Some(ResultValue::Oid(ti.oid)),
                Some(ResultValue::Text(path.to_string())),
            ),
            None => (None, None),
        };
        let size = if ti.size >= 0 {
            Some(ResultValue::Int8(ti.size / 1024))
        } else {
            None
        };
        seam::do_tup_output::call(tstate, vec![spcoid, spclocation, size]);
    }

    seam::end_tup_output::call(tstate);
    Ok(())
}

fn format_lsn(lsn: XLogRecPtr) -> String {
    let hi = (lsn >> 32) as u32;
    let lo = lsn as u32;
    format!("{hi:X}/{lo:X}")
}

fn timestamp_difference_milliseconds(start: TimestampTz, stop: TimestampTz) -> i64 {
    if start >= stop {
        return 0;
    }
    let diff = stop - start;
    if diff >= (i32::MAX as i64 - 999) * 1000 {
        i32::MAX as i64
    } else {
        (diff + 999) / 1000
    }
}

pub fn init_seams() {}

#[cfg(test)]
mod tests;
