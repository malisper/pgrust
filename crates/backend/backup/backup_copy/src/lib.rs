//! Port of PostgreSQL's `basebackup_copy` (`src/backend/backup/
//! basebackup_copy.c`): the COPY-protocol base-backup [`Bbsink`].
//!
//! We send a result set with information about the tablespaces to be included
//! in the backup before starting COPY OUT. Then, we start a single COPY OUT
//! operation and transmit all the archives and the manifest if present during
//! the course of that single COPY OUT. Each `CopyData` message begins with a
//! type byte, allowing us to signal the start of a new archive, or the
//! manifest, by some means other than ending the COPY stream. This also allows
//! for future protocol extensions, since we can include arbitrary information
//! in the message stream as long as we're certain that the client will know
//! what to do with it.
//!
//! An older method that sent each archive using a separate COPY OUT operation
//! is no longer supported (so there is no `bbsink_copytblspc`).
//!
//! All of the file's own logic is ported here over the owned [`BbsinkOps`]
//! trait: the start-of-backup / end-of-backup wire-protocol sequence, the
//! in-band `CopyData` type-byte framing (`'n'` new archive, `'d'` data, `'m'`
//! manifest, `'p'` progress report), the progress-report timing policy (a
//! byte-interval gate plus a one-second wall-clock threshold), the `"%X/%X"`
//! LSN text formatting, and the column layout / value selection of the two
//! `DestRemoteSimple` result sets.
//!
//! The libpq message assembly/output (`pq_beginmessage` / `pq_send*` /
//! `pq_endmessage` / `pq_putmessage` / `pq_putemptymessage` /
//! `pq_puttextmessage`) is reached through the real, landed
//! `backend-libpq-pqformat` + `backend-libpq-pqcomm` crates;
//! `pq_flush_if_writable` and the wall clock (`GetCurrentTimestamp` /
//! `TimestampDifferenceMilliseconds`) through their owners' seams. The
//! `DestReceiver` result-set path (`CreateDestReceiver(DestRemoteSimple)` +
//! `begin/do/end_tup_output`) is seamed through
//! [`backup_copy_seams`] and panics until the receiver-value router
//! keystone lands.
//!
//! # The C `msgbuffer` trick
//!
//! ```c
//! typedef struct bbsink_copystream
//! {
//!     bbsink      base;
//!     bool        send_to_client;
//!     char       *msgbuffer;
//!     TimestampTz last_progress_report_time;
//!     uint64      bytes_done_at_last_time_check;
//! } bbsink_copystream;
//! ```
//!
//! The C code over-allocates the working buffer by `MAXIMUM_ALIGNOF` bytes so
//! it can stash the leading `'d'` (in-band data type) byte just before the
//! aligned region `bbs_buffer` and ship type-byte-plus-data with a single
//! zero-copy `pq_putmessage('d', msgbuffer, len + 1)`. That is purely a
//! copy-avoidance optimization below the libpq boundary. Here
//! [`pq_putmessage`](::pqcomm_seams::pq_putmessage) takes the
//! complete body, so the `'d'` byte is prepended to the buffer slice when
//! assembling the message; the bytes on the wire are identical. The
//! `bbsink base` (forwarding chain — empty for this leaf — and the working
//! buffer) is provided by the surrounding [`Bbsink`]; the `send_to_client`
//! flag, the surrounding `MemoryContext` handle (for message assembly), and the
//! two progress-timer fields live in [`BbsinkCopystream`].

#![no_std]
#![forbid(unsafe_code)]

extern crate alloc;
#[cfg(test)]
extern crate std;

use alloc::boxed::Box;
use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;

use ::sink::{Bbsink, BbsinkOps, BbsinkState};
use ::pqcomm_seams::{pq_flush_if_writable, pq_putmessage};
use ::pqformat::{
    pq_beginmessage, pq_endmessage, pq_putemptymessage, pq_puttextmessage, pq_sendbyte,
    pq_sendint16, pq_sendint64, pq_sendstring,
};
use ::timestamp_seams::{get_current_timestamp, timestamp_difference_milliseconds};
use ::mcx::Mcx;
use ::types_core::primitive::{Size, TimeLineID, TimestampTz, XLogRecPtr};
use ::types_error::PgResult;

use backup_copy_seams as seam;
use ::backup_copy_seams::{ResultColumn, ResultColumnType, ResultValue};

/// Builtin type OIDs (`catalog/pg_type_d.h`), used for the two result sets.
const TEXTOID: ResultColumnType = ResultColumnType::Text;
const INT8OID: ResultColumnType = ResultColumnType::Int8;
const OIDOID: ResultColumnType = ResultColumnType::Oid;

/// `CommandComplete` tag closing each `DestRemoteSimple` result set.
const SELECT_TAG: &[u8] = b"SELECT";

/// libpq protocol message type bytes (`libpq/protocol.h`).
const PQ_MSG_COMMAND_COMPLETE: u8 = b'C';
const PQ_MSG_COPY_DATA: u8 = b'd';
const PQ_MSG_COPY_DONE: u8 = b'c';
const PQ_MSG_COPY_OUT_RESPONSE: u8 = b'H';

// We don't want to send progress messages to the client excessively
// frequently. Ideally, we'd like to send a message when the time since the last
// message reaches PROGRESS_REPORT_MILLISECOND_THRESHOLD, but checking the
// system time every time we send a tiny bit of data seems too expensive. So we
// only check it after the number of bytes since the last check reaches
// PROGRESS_REPORT_BYTE_INTERVAL.
const PROGRESS_REPORT_BYTE_INTERVAL: u64 = 65536;
const PROGRESS_REPORT_MILLISECOND_THRESHOLD: i64 = 1000;

/// The COPY-stream base-backup sink (C `bbsink_copystream`).
///
/// Holds the `send_to_client` flag, the surrounding memory-context handle (the
/// C `CurrentMemoryContext` in which `pq_beginmessage`'s `StringInfo` is
/// `initStringInfo`-ed), and the progress-report timer state. The (empty, leaf)
/// forwarding chain and the working buffer are owned by the surrounding
/// [`Bbsink`] this is installed into. Construct the sink with
/// [`bbsink_copystream_new`].
pub struct BbsinkCopystream<'mcx> {
    /// Are we sending the archives to the client, or somewhere else? (C
    /// `send_to_client`.)
    send_to_client: bool,
    /// Memory context for libpq message assembly (the C
    /// `CurrentMemoryContext`).
    mcx: Mcx<'mcx>,
    /// When did we last report progress to the client (C
    /// `last_progress_report_time`).
    last_progress_report_time: TimestampTz,
    /// How much progress had been done at the last system-time check (C
    /// `bytes_done_at_last_time_check`).
    bytes_done_at_last_time_check: u64,
}

/// Create a new 'copystream' bbsink (C `bbsink_copystream_new`).
///
/// `send_to_client` mirrors the C field of the same name. `mcx` is the
/// surrounding memory context (into which the sink's working buffer is
/// allocated and in which libpq messages are assembled). The progress timer is
/// primed with the current wall-clock time, exactly as the C code does after
/// `palloc0`-zeroing the struct.
pub fn bbsink_copystream_new<'mcx>(mcx: Mcx<'mcx>, send_to_client: bool) -> Box<Bbsink<'mcx>> {
    // bbsink_copystream *sink = palloc0(sizeof(bbsink_copystream));
    // *((const bbsink_ops **) &sink->base.bbs_ops) = &bbsink_copystream_ops;
    // sink->send_to_client = send_to_client;
    let ops = BbsinkCopystream {
        send_to_client,
        mcx,
        // sink->last_progress_report_time = GetCurrentTimestamp();
        last_progress_report_time: get_current_timestamp::call(),
        // sink->bytes_done_at_last_time_check = UINT64CONST(0);
        bytes_done_at_last_time_check: 0,
    };
    // A leaf sink: no successor. `&sink->base` in C.
    Box::new(Bbsink::new(mcx, Box::new(ops), None))
}

impl<'mcx> BbsinkOps<'mcx> for BbsinkCopystream<'mcx> {
    /// C `bbsink_copystream_begin_backup`: send the start-of-backup wire
    /// protocol messages.
    fn begin_backup(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        // Initialize buffer. The C code over-allocates by MAXIMUM_ALIGNOF so it
        // can slip the leading 'd' type byte in just before the aligned region
        // with no copy; here `pq_putmessage` takes the complete body, so a plain
        // buffer of the requested length suffices (the 'd' byte is prepended
        // when the message is assembled). `bbs_buffer_length` was set by
        // `bbsink_begin_backup` before this callback.
        let buffer_length = sink.buffer_length();
        sink.set_buffer(self.mcx, buffer_length)?;

        // Tell client the backup start location.
        send_xlog_rec_ptr_result(self.mcx, state.startptr, state.starttli)?;

        // Send client a list of tablespaces.
        send_tablespace_list(self.mcx, state)?;

        // Send a CommandComplete message.
        pq_puttextmessage(self.mcx, PQ_MSG_COMMAND_COMPLETE, SELECT_TAG)?;

        // Begin COPY stream. This will be used for all archives + manifest.
        send_copy_out_response(self.mcx)
    }

    /// C `bbsink_copystream_begin_archive`: send a CopyData message announcing
    /// the beginning of a new archive.
    fn begin_archive(
        &mut self,
        _sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        archive_name: &str,
    ) -> PgResult<()> {
        // ti = list_nth(state->tablespaces, state->tablespace_num);
        let ti = &state.tablespaces[state.tablespace_num as usize];

        // pq_beginmessage(&buf, PqMsg_CopyData);
        let mut buf = pq_beginmessage(self.mcx, PQ_MSG_COPY_DATA)?;
        pq_sendbyte(&mut buf, b'n')?; // New archive
        pq_sendstring(&mut buf, archive_name.as_bytes())?;
        // ti->path == NULL ? "" : ti->path
        let path = ti.path.as_deref().unwrap_or("");
        pq_sendstring(&mut buf, path.as_bytes())?;
        pq_endmessage(buf)
    }

    /// C `bbsink_copystream_archive_contents`: send a CopyData message
    /// containing a chunk of archive content, and consider a progress report.
    fn archive_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        // Send the archive content to the client, if appropriate.
        if self.send_to_client {
            // Add one because we're also sending a leading type byte. C ships
            // msgbuffer[0]='d' followed by `len` data bytes in a single
            // pq_putmessage('d', msgbuffer, len + 1); here we assemble the same
            // CopyData body (in-band 'd' type byte + data).
            let mut body: Vec<u8> = Vec::with_capacity(len + 1);
            body.push(b'd');
            body.extend_from_slice(sink.buffer_slice(len));
            let _eof = pq_putmessage::call(PQ_MSG_COPY_DATA, &body)?;
        }

        // Consider whether to send a progress report to the client.
        // C: uint64 addition (wraps); guard the debug overflow check.
        let targetbytes = self
            .bytes_done_at_last_time_check
            .wrapping_add(PROGRESS_REPORT_BYTE_INTERVAL);
        if targetbytes <= state.bytes_done {
            let now = get_current_timestamp::call();

            // OK, we've sent a decent number of bytes, so check the system time
            // to see whether we're due to send a progress report.
            self.bytes_done_at_last_time_check = state.bytes_done;
            let ms = timestamp_difference_milliseconds::call(self.last_progress_report_time, now);

            // Send a progress report if enough time has passed. Also send one if
            // the system clock was set backward, so that such occurrences don't
            // have the effect of suppressing further progress messages.
            if ms >= PROGRESS_REPORT_MILLISECOND_THRESHOLD || now < self.last_progress_report_time {
                self.last_progress_report_time = now;

                let mut buf = pq_beginmessage(self.mcx, PQ_MSG_COPY_DATA)?;
                pq_sendbyte(&mut buf, b'p')?; // Progress report
                pq_sendint64(&mut buf, state.bytes_done)?;
                pq_endmessage(buf)?;
                let _pending = pq_flush_if_writable::call();
            }
        }
        Ok(())
    }

    /// C `bbsink_copystream_end_archive`: we don't need to explicitly signal the
    /// end of the archive; the client will figure out that we've reached the end
    /// when we begin the next one, or begin the manifest, or end the COPY
    /// stream. However, this is a good time to force out a progress report — one
    /// reason is that on the last archive, if we don't, the client would never
    /// be told that we sent all the bytes.
    fn end_archive(&mut self, _sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        self.bytes_done_at_last_time_check = state.bytes_done;
        self.last_progress_report_time = get_current_timestamp::call();
        let mut buf = pq_beginmessage(self.mcx, PQ_MSG_COPY_DATA)?;
        pq_sendbyte(&mut buf, b'p')?; // Progress report
        pq_sendint64(&mut buf, state.bytes_done)?;
        pq_endmessage(buf)?;
        let _pending = pq_flush_if_writable::call();
        Ok(())
    }

    /// C `bbsink_copystream_begin_manifest`: send a CopyData message announcing
    /// the beginning of the backup manifest.
    fn begin_manifest(&mut self, _sink: &mut Bbsink<'mcx>, _state: &mut BbsinkState) -> PgResult<()> {
        let mut buf = pq_beginmessage(self.mcx, PQ_MSG_COPY_DATA)?;
        pq_sendbyte(&mut buf, b'm')?; // Manifest
        pq_endmessage(buf)
    }

    /// C `bbsink_copystream_manifest_contents`: each chunk of manifest data is
    /// sent using a CopyData message.
    fn manifest_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        _state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        if self.send_to_client {
            // Add one because we're also sending a leading type byte.
            let mut body: Vec<u8> = Vec::with_capacity(len + 1);
            body.push(b'd');
            body.extend_from_slice(sink.buffer_slice(len));
            let _eof = pq_putmessage::call(PQ_MSG_COPY_DATA, &body)?;
        }
        Ok(())
    }

    /// C `bbsink_copystream_end_manifest`: we don't need an explicit terminator
    /// for the backup manifest. Do nothing.
    fn end_manifest(&mut self, _sink: &mut Bbsink<'mcx>, _state: &mut BbsinkState) -> PgResult<()> {
        // Do nothing.
        Ok(())
    }

    /// C `bbsink_copystream_end_backup`: send the end-of-backup wire protocol
    /// messages.
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

    /// C `bbsink_copystream_cleanup`: nothing to do.
    fn cleanup(&mut self, _sink: &mut Bbsink<'mcx>, _state: &mut BbsinkState) -> PgResult<()> {
        // Nothing to do.
        Ok(())
    }
}

/// C `SendCopyOutResponse`: send a `CopyOutResponse` message that begins the
/// single COPY-out stream. C sends overall format byte 0 and natts 0.
fn send_copy_out_response(mcx: Mcx<'_>) -> PgResult<()> {
    let mut buf = pq_beginmessage(mcx, PQ_MSG_COPY_OUT_RESPONSE)?;
    pq_sendbyte(&mut buf, 0)?; // overall format
    pq_sendint16(&mut buf, 0)?; // natts
    pq_endmessage(buf)
}

/// C `SendCopyDone`: send a `CopyDone` message that ends the COPY-out stream.
fn send_copy_done() -> PgResult<()> {
    pq_putemptymessage(PQ_MSG_COPY_DONE)
}

/// C `SendXlogRecPtrResult`: send a single resultset containing just a single
/// `XLogRecPtr` record (in text format) plus the timeline.
///
/// int8 may seem like a surprising data type for the timeline, but in theory
/// int4 would not be wide enough for this, as `TimeLineID` is unsigned.
fn send_xlog_rec_ptr_result(mcx: Mcx<'_>, ptr: XLogRecPtr, tli: TimeLineID) -> PgResult<()> {
    // dest = CreateDestReceiver(DestRemoteSimple);
    let dest = seam::create_dest_remote_simple::call();

    // tupdesc = CreateTemplateTupleDesc(2);
    // TupleDescInitBuiltinEntry(tupdesc, 1, "recptr", TEXTOID, -1, 0);
    // TupleDescInitBuiltinEntry(tupdesc, 2, "tli", INT8OID, -1, 0);
    let columns = vec![
        ResultColumn { name: "recptr".to_string(), typ: TEXTOID },
        ResultColumn { name: "tli".to_string(), typ: INT8OID },
    ];

    // send RowDescription:
    // tstate = begin_tup_output_tupdesc(dest, tupdesc, &TTSOpsVirtual);
    let tstate = seam::begin_tup_output_tupdesc::call(dest, columns);

    // Data row:
    // values[0] = CStringGetTextDatum(psprintf("%X/%X", LSN_FORMAT_ARGS(ptr)));
    // values[1] = Int64GetDatum(tli);
    let values = vec![
        Some(ResultValue::Text(format_lsn(ptr))),
        Some(ResultValue::Int8(tli as i64)),
    ];
    seam::do_tup_output::call(tstate, values);

    // end_tup_output(tstate);
    seam::end_tup_output::call(tstate);

    // Send a CommandComplete message.
    pq_puttextmessage(mcx, PQ_MSG_COMMAND_COMPLETE, SELECT_TAG)
}

/// C `SendTablespaceList`: send a result set via libpq describing the
/// tablespace list. Columns are `(spcoid oid, spclocation text, size int8)`.
fn send_tablespace_list(_mcx: Mcx<'_>, state: &BbsinkState) -> PgResult<()> {
    // dest = CreateDestReceiver(DestRemoteSimple);
    let dest = seam::create_dest_remote_simple::call();

    // tupdesc = CreateTemplateTupleDesc(3);
    // TupleDescInitBuiltinEntry(tupdesc, 1, "spcoid", OIDOID, -1, 0);
    // TupleDescInitBuiltinEntry(tupdesc, 2, "spclocation", TEXTOID, -1, 0);
    // TupleDescInitBuiltinEntry(tupdesc, 3, "size", INT8OID, -1, 0);
    let columns = vec![
        ResultColumn { name: "spcoid".to_string(), typ: OIDOID },
        ResultColumn { name: "spclocation".to_string(), typ: TEXTOID },
        ResultColumn { name: "size".to_string(), typ: INT8OID },
    ];

    // send RowDescription.
    let tstate = seam::begin_tup_output_tupdesc::call(dest, columns);

    // Construct and send the directory information.
    for ti in &state.tablespaces {
        // Send one datarow message.
        // if (ti->path == NULL) { nulls[0] = true; nulls[1] = true; }
        // else { values[0] = ObjectIdGetDatum(ti->oid);
        //        values[1] = CStringGetTextDatum(ti->path); }
        let (spcoid, spclocation) = match ti.path.as_deref() {
            Some(path) => (
                Some(ResultValue::Oid(ti.oid)),
                Some(ResultValue::Text(path.to_string())),
            ),
            None => (None, None),
        };
        // if (ti->size >= 0) values[2] = Int64GetDatum(ti->size / 1024);
        // else nulls[2] = true;
        let size = match ti.size {
            Some(size) if size >= 0 => Some(ResultValue::Int8(size / 1024)),
            _ => None,
        };

        seam::do_tup_output::call(tstate, vec![spcoid, spclocation, size]);
    }

    // end_tup_output(tstate);
    seam::end_tup_output::call(tstate);
    Ok(())
}

/// Format an LSN as the C `psprintf("%X/%X", LSN_FORMAT_ARGS(lsn))` string: the
/// high 32 bits and the low 32 bits as uppercase hex, joined by `/`, with no
/// zero-padding (matching C's `%X`).
fn format_lsn(lsn: XLogRecPtr) -> String {
    let hi = (lsn >> 32) as u32;
    let lo = lsn as u32;
    alloc::format!("{hi:X}/{lo:X}")
}

/// Install this crate's seam implementations. This unit owns no inward seams:
/// its public entry point is the plain `bbsink_copystream_new` constructor,
/// which `basebackup.c` will call directly (or wire through its own sink
/// constructor seam) when it lands — mirroring `basebackup_server`, which also
/// installs nothing.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
