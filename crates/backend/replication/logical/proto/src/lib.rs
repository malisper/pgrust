// proto.c: the logical replication wire protocol (protocol version 4).
//
// Writers emit into the decoding context's OutBuf byte buffer (C: StringInfo
// ctx->out) via pq_send* parity helpers; readers consume a received message
// body through Reader (C: the StringInfo cursor). Where C passes a
// ReorderBufferTXN* the writers take the specific txn fields instead — the
// pgrust plugin surface hands plugins (rb, TxnId), and keeping the writers
// field-scalar keeps this crate unit-testable without a reorderbuffer.
// Tuples are written from the reorderbuffer's HeapTupleData (C converts to a
// TupleTableSlot first; the data is identical).
//
// Streaming (stream_*) and two-phase (prepare family) messages: the message
// type constants exist, the write/read functions refuse loudly (phase-2).
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use datum::Datum;
use mcx::Mcx;
use types_core::{
    InvalidOid, InvalidTransactionId, InvalidXLogRecPtr, Oid, TimestampTz, TransactionId,
    XLogRecPtr,
};
use types_error::{PgResult, ERROR};
use types_rel::pg_class::{REPLICA_IDENTITY_DEFAULT, REPLICA_IDENTITY_FULL, REPLICA_IDENTITY_INDEX};
use types_rel::RelationData;
use types_tuple::{varatt, FormData_pg_attribute, HeapTupleData};

use elog::elog;

// Protocol capabilities (logicalproto.h).
pub const LOGICALREP_PROTO_MIN_VERSION_NUM: u32 = 1;
pub const LOGICALREP_PROTO_VERSION_NUM: u32 = 1;
pub const LOGICALREP_PROTO_STREAM_VERSION_NUM: u32 = 2;
pub const LOGICALREP_PROTO_TWOPHASE_VERSION_NUM: u32 = 3;
pub const LOGICALREP_PROTO_STREAM_PARALLEL_VERSION_NUM: u32 = 4;
pub const LOGICALREP_PROTO_MAX_VERSION_NUM: u32 = LOGICALREP_PROTO_STREAM_PARALLEL_VERSION_NUM;

// LogicalRepMsgType (logicalproto.h): single-byte wire message tags.
pub const LOGICAL_REP_MSG_BEGIN: u8 = b'B';
pub const LOGICAL_REP_MSG_COMMIT: u8 = b'C';
pub const LOGICAL_REP_MSG_ORIGIN: u8 = b'O';
pub const LOGICAL_REP_MSG_INSERT: u8 = b'I';
pub const LOGICAL_REP_MSG_UPDATE: u8 = b'U';
pub const LOGICAL_REP_MSG_DELETE: u8 = b'D';
pub const LOGICAL_REP_MSG_TRUNCATE: u8 = b'T';
pub const LOGICAL_REP_MSG_RELATION: u8 = b'R';
pub const LOGICAL_REP_MSG_TYPE: u8 = b'Y';
pub const LOGICAL_REP_MSG_MESSAGE: u8 = b'M';
pub const LOGICAL_REP_MSG_BEGIN_PREPARE: u8 = b'b';
pub const LOGICAL_REP_MSG_PREPARE: u8 = b'P';
pub const LOGICAL_REP_MSG_COMMIT_PREPARED: u8 = b'K';
pub const LOGICAL_REP_MSG_ROLLBACK_PREPARED: u8 = b'r';
pub const LOGICAL_REP_MSG_STREAM_START: u8 = b'S';
pub const LOGICAL_REP_MSG_STREAM_STOP: u8 = b'E';
pub const LOGICAL_REP_MSG_STREAM_COMMIT: u8 = b'c';
pub const LOGICAL_REP_MSG_STREAM_ABORT: u8 = b'A';
pub const LOGICAL_REP_MSG_STREAM_PREPARE: u8 = b'p';

// LogicalRepTupleData.colstatus values (also on the wire).
pub const LOGICALREP_COLUMN_NULL: u8 = b'n';
pub const LOGICALREP_COLUMN_UNCHANGED: u8 = b'u';
pub const LOGICALREP_COLUMN_TEXT: u8 = b't';
pub const LOGICALREP_COLUMN_BINARY: u8 = b'b';

// proto.c flag bytes.
pub const LOGICALREP_IS_REPLICA_IDENTITY: u8 = 1;
pub const MESSAGE_TRANSACTIONAL: u8 = 1 << 0;
pub const TRUNCATE_CASCADE: u8 = 1 << 0;
pub const TRUNCATE_RESTART_SEQS: u8 = 1 << 1;

// PublishGencolsType (pg_publication.h).
pub const PUBLISH_GENCOLS_NONE: u8 = b'n';
pub const PUBLISH_GENCOLS_STORED: u8 = b's';

const ATTRIBUTE_GENERATED_STORED: i8 = b's' as i8;

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported callee reached from proto.c: {what} (phase-2)")
}

pub type LogicalRepRelId = u32;

/// A tuple received via logical replication; columns are the REMOTE table's.
/// colvalues[i] is Some for TEXT/BINARY columns (no trailing NUL) and None
/// for NULL/UNCHANGED.
#[derive(Clone)]
pub struct LogicalRepTupleData {
    pub colvalues: Vec<Option<Vec<u8>>>,
    pub colstatus: Vec<u8>,
    pub ncols: usize,
}

/// Relation metadata received via logical replication. attkeys\[i\] is true
/// when column i (0-based remote index) is part of the replica identity (C's
/// attkeys Bitmapset).
#[derive(Clone)]
pub struct LogicalRepRelation {
    pub remoteid: LogicalRepRelId,
    pub nspname: String,
    pub relname: String,
    pub natts: usize,
    pub attnames: Vec<String>,
    pub atttyps: Vec<Oid>,
    pub replident: u8,
    pub relkind: u8,
    pub attkeys: Vec<bool>,
}

#[derive(Clone)]
pub struct LogicalRepTyp {
    pub remoteid: Oid,
    pub nspname: String,
    pub typname: String,
}

#[derive(Clone)]
pub struct LogicalRepBeginData {
    pub final_lsn: XLogRecPtr,
    pub committime: TimestampTz,
    pub xid: TransactionId,
}

#[derive(Clone)]
pub struct LogicalRepCommitData {
    pub commit_lsn: XLogRecPtr,
    pub end_lsn: XLogRecPtr,
    pub committime: TimestampTz,
}

// --- pq_send* parity helpers over the raw out buffer ---

fn send_byte(out: &mut Vec<u8>, b: u8) {
    out.push(b);
}
fn send_int8(out: &mut Vec<u8>, v: u8) {
    out.push(v);
}
fn send_int16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_be_bytes());
}
fn send_int32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_be_bytes());
}
fn send_int64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_be_bytes());
}
// pq_sendstring: bytes + NUL terminator.
fn send_string(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(s.as_bytes());
    out.push(0);
}
// pq_sendcountedtext: int32 length + bytes (no NUL).
fn send_countedtext(out: &mut Vec<u8>, s: &[u8]) {
    send_int32(out, s.len() as u32);
    out.extend_from_slice(s);
}

/// The read cursor over a received logical replication message body
/// (C: StringInfo with cursor; the walsender's MsgReader is the same shape).
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }
    fn need(&self, n: usize) -> PgResult<()> {
        if self.pos + n > self.buf.len() {
            elog(ERROR, "insufficient data left in message".to_string())?;
        }
        Ok(())
    }
    pub fn get_byte(&mut self) -> PgResult<u8> {
        self.need(1)?;
        let b = self.buf[self.pos];
        self.pos += 1;
        Ok(b)
    }
    pub fn get_int16(&mut self) -> PgResult<u16> {
        self.need(2)?;
        let v = u16::from_be_bytes(self.buf[self.pos..self.pos + 2].try_into().unwrap());
        self.pos += 2;
        Ok(v)
    }
    pub fn get_int32(&mut self) -> PgResult<u32> {
        self.need(4)?;
        let v = u32::from_be_bytes(self.buf[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(v)
    }
    pub fn get_int64(&mut self) -> PgResult<u64> {
        self.need(8)?;
        let v = u64::from_be_bytes(self.buf[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }
    // pq_getmsgstring: NUL-terminated string.
    pub fn get_string(&mut self) -> PgResult<String> {
        let start = self.pos;
        while self.pos < self.buf.len() && self.buf[self.pos] != 0 {
            self.pos += 1;
        }
        if self.pos >= self.buf.len() {
            elog(ERROR, "invalid string in message".to_string())?;
        }
        let s = String::from_utf8_lossy(&self.buf[start..self.pos]).into_owned();
        self.pos += 1; // NUL
        Ok(s)
    }
    pub fn get_bytes(&mut self, n: usize) -> PgResult<Vec<u8>> {
        self.need(n)?;
        let v = self.buf[self.pos..self.pos + n].to_vec();
        self.pos += n;
        Ok(v)
    }
}

// --- BEGIN ---

/// logicalrep_write_begin: fields are C's txn->final_lsn,
/// txn->xact_time.commit_time, txn->xid.
pub fn logicalrep_write_begin(
    out: &mut Vec<u8>,
    final_lsn: XLogRecPtr,
    commit_time: TimestampTz,
    xid: TransactionId,
) {
    send_byte(out, LOGICAL_REP_MSG_BEGIN);
    send_int64(out, final_lsn);
    send_int64(out, commit_time as u64);
    send_int32(out, xid);
}

pub fn logicalrep_read_begin(r: &mut Reader<'_>) -> PgResult<LogicalRepBeginData> {
    let final_lsn = r.get_int64()?;
    if final_lsn == InvalidXLogRecPtr {
        elog(ERROR, "final_lsn not set in begin message".to_string())?;
    }
    Ok(LogicalRepBeginData {
        final_lsn,
        committime: r.get_int64()? as TimestampTz,
        xid: r.get_int32()?,
    })
}

// --- COMMIT ---

/// logicalrep_write_commit: end_lsn/commit_time are C's txn fields.
pub fn logicalrep_write_commit(
    out: &mut Vec<u8>,
    commit_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
    commit_time: TimestampTz,
) {
    send_byte(out, LOGICAL_REP_MSG_COMMIT);
    send_byte(out, 0); // flags, unused
    send_int64(out, commit_lsn);
    send_int64(out, end_lsn);
    send_int64(out, commit_time as u64);
}

pub fn logicalrep_read_commit(r: &mut Reader<'_>) -> PgResult<LogicalRepCommitData> {
    let flags = r.get_byte()?;
    if flags != 0 {
        elog(ERROR, format!("unrecognized flags {flags} in commit message"))?;
    }
    Ok(LogicalRepCommitData {
        commit_lsn: r.get_int64()?,
        end_lsn: r.get_int64()?,
        committime: r.get_int64()? as TimestampTz,
    })
}

// --- ORIGIN ---

pub fn logicalrep_write_origin(out: &mut Vec<u8>, origin: &str, origin_lsn: XLogRecPtr) {
    send_byte(out, LOGICAL_REP_MSG_ORIGIN);
    send_int64(out, origin_lsn);
    send_string(out, origin);
}

pub fn logicalrep_read_origin(r: &mut Reader<'_>) -> PgResult<(String, XLogRecPtr)> {
    let origin_lsn = r.get_int64()?;
    Ok((r.get_string()?, origin_lsn))
}

// --- INSERT / UPDATE / DELETE ---

pub fn logicalrep_write_insert(
    out: &mut Vec<u8>,
    xid: TransactionId,
    rel: &RelationData<'static>,
    newtuple: &HeapTupleData<'_>,
    binary: bool,
    columns: Option<&[i16]>,
    include_gencols_type: u8,
) -> PgResult<()> {
    send_byte(out, LOGICAL_REP_MSG_INSERT);
    if xid != InvalidTransactionId {
        send_int32(out, xid);
    }
    send_int32(out, rel.rd_id);
    send_byte(out, b'N');
    logicalrep_write_tuple(out, rel, newtuple, binary, columns, include_gencols_type)
}

pub fn logicalrep_read_insert(
    r: &mut Reader<'_>,
) -> PgResult<(LogicalRepRelId, LogicalRepTupleData)> {
    let relid = r.get_int32()?;
    let action = r.get_byte()?;
    if action != b'N' {
        elog(ERROR, format!("expected new tuple but got {action}"))?;
    }
    Ok((relid, logicalrep_read_tuple(r)?))
}

pub fn logicalrep_write_update(
    out: &mut Vec<u8>,
    xid: TransactionId,
    rel: &RelationData<'static>,
    oldtuple: Option<&HeapTupleData<'_>>,
    newtuple: &HeapTupleData<'_>,
    binary: bool,
    columns: Option<&[i16]>,
    include_gencols_type: u8,
) -> PgResult<()> {
    send_byte(out, LOGICAL_REP_MSG_UPDATE);

    debug_assert!(
        rel.rd_rel.relreplident == REPLICA_IDENTITY_DEFAULT
            || rel.rd_rel.relreplident == REPLICA_IDENTITY_FULL
            || rel.rd_rel.relreplident == REPLICA_IDENTITY_INDEX
    );

    if xid != InvalidTransactionId {
        send_int32(out, xid);
    }
    send_int32(out, rel.rd_id);

    if let Some(old) = oldtuple {
        if rel.rd_rel.relreplident == REPLICA_IDENTITY_FULL {
            send_byte(out, b'O');
        } else {
            send_byte(out, b'K');
        }
        logicalrep_write_tuple(out, rel, old, binary, columns, include_gencols_type)?;
    }

    send_byte(out, b'N');
    logicalrep_write_tuple(out, rel, newtuple, binary, columns, include_gencols_type)
}

pub struct ReadUpdateResult {
    pub relid: LogicalRepRelId,
    pub has_oldtuple: bool,
    pub oldtup: Option<LogicalRepTupleData>,
    pub newtup: LogicalRepTupleData,
}

pub fn logicalrep_read_update(r: &mut Reader<'_>) -> PgResult<ReadUpdateResult> {
    let relid = r.get_int32()?;
    let mut action = r.get_byte()?;
    if action != b'K' && action != b'O' && action != b'N' {
        elog(
            ERROR,
            format!("expected action 'N', 'O' or 'K', got {}", action as char),
        )?;
    }

    let mut has_oldtuple = false;
    let mut oldtup = None;
    if action == b'K' || action == b'O' {
        oldtup = Some(logicalrep_read_tuple(r)?);
        has_oldtuple = true;
        action = r.get_byte()?;
    }

    if action != b'N' {
        elog(ERROR, format!("expected action 'N', got {}", action as char))?;
    }

    Ok(ReadUpdateResult {
        relid,
        has_oldtuple,
        oldtup,
        newtup: logicalrep_read_tuple(r)?,
    })
}

pub fn logicalrep_write_delete(
    out: &mut Vec<u8>,
    xid: TransactionId,
    rel: &RelationData<'static>,
    oldtuple: &HeapTupleData<'_>,
    binary: bool,
    columns: Option<&[i16]>,
    include_gencols_type: u8,
) -> PgResult<()> {
    debug_assert!(
        rel.rd_rel.relreplident == REPLICA_IDENTITY_DEFAULT
            || rel.rd_rel.relreplident == REPLICA_IDENTITY_FULL
            || rel.rd_rel.relreplident == REPLICA_IDENTITY_INDEX
    );

    send_byte(out, LOGICAL_REP_MSG_DELETE);
    if xid != InvalidTransactionId {
        send_int32(out, xid);
    }
    send_int32(out, rel.rd_id);

    if rel.rd_rel.relreplident == REPLICA_IDENTITY_FULL {
        send_byte(out, b'O');
    } else {
        send_byte(out, b'K');
    }
    logicalrep_write_tuple(out, rel, oldtuple, binary, columns, include_gencols_type)
}

pub fn logicalrep_read_delete(
    r: &mut Reader<'_>,
) -> PgResult<(LogicalRepRelId, LogicalRepTupleData)> {
    let relid = r.get_int32()?;
    let action = r.get_byte()?;
    if action != b'K' && action != b'O' {
        elog(
            ERROR,
            format!("expected action 'O' or 'K', got {}", action as char),
        )?;
    }
    Ok((relid, logicalrep_read_tuple(r)?))
}

// --- TRUNCATE ---

pub fn logicalrep_write_truncate(
    out: &mut Vec<u8>,
    xid: TransactionId,
    relids: &[Oid],
    cascade: bool,
    restart_seqs: bool,
) {
    send_byte(out, LOGICAL_REP_MSG_TRUNCATE);
    if xid != InvalidTransactionId {
        send_int32(out, xid);
    }
    send_int32(out, relids.len() as u32);

    let mut flags: u8 = 0;
    if cascade {
        flags |= TRUNCATE_CASCADE;
    }
    if restart_seqs {
        flags |= TRUNCATE_RESTART_SEQS;
    }
    send_int8(out, flags);

    for &relid in relids {
        send_int32(out, relid);
    }
}

pub fn logicalrep_read_truncate(r: &mut Reader<'_>) -> PgResult<(Vec<Oid>, bool, bool)> {
    let nrelids = r.get_int32()? as usize;
    let flags = r.get_byte()?;
    let cascade = flags & TRUNCATE_CASCADE != 0;
    let restart_seqs = flags & TRUNCATE_RESTART_SEQS != 0;
    let mut relids = Vec::with_capacity(nrelids);
    for _ in 0..nrelids {
        relids.push(r.get_int32()?);
    }
    Ok((relids, cascade, restart_seqs))
}

// --- MESSAGE ---

pub fn logicalrep_write_message(
    out: &mut Vec<u8>,
    xid: TransactionId,
    lsn: XLogRecPtr,
    transactional: bool,
    prefix: &str,
    message: &[u8],
) {
    send_byte(out, LOGICAL_REP_MSG_MESSAGE);

    let mut flags: u8 = 0;
    if transactional {
        flags |= MESSAGE_TRANSACTIONAL;
    }

    if xid != InvalidTransactionId {
        send_int32(out, xid);
    }

    send_int8(out, flags);
    send_int64(out, lsn);
    send_string(out, prefix);
    send_int32(out, message.len() as u32);
    out.extend_from_slice(message);
}

// --- RELATION ---

pub fn logicalrep_write_rel(
    out: &mut Vec<u8>,
    xid: TransactionId,
    rel: &RelationData<'static>,
    columns: Option<&[i16]>,
    include_gencols_type: u8,
) -> PgResult<()> {
    send_byte(out, LOGICAL_REP_MSG_RELATION);
    if xid != InvalidTransactionId {
        send_int32(out, xid);
    }
    send_int32(out, rel.rd_id);

    logicalrep_write_namespace(out, rel.rd_rel.relnamespace)?;
    let relname = String::from_utf8_lossy(rel.rd_rel.relname.name_str()).into_owned();
    send_string(out, &relname);

    send_byte(out, rel.rd_rel.relreplident);

    logicalrep_write_attrs(out, rel, columns, include_gencols_type)
}

pub fn logicalrep_read_rel(r: &mut Reader<'_>) -> PgResult<LogicalRepRelation> {
    let remoteid = r.get_int32()?;
    let nspname = logicalrep_read_namespace(r)?;
    let relname = r.get_string()?;
    let replident = r.get_byte()?;
    let (natts, attnames, atttyps, attkeys) = logicalrep_read_attrs(r)?;
    Ok(LogicalRepRelation {
        remoteid,
        nspname,
        relname,
        natts,
        attnames,
        atttyps,
        replident,
        // The remote relkind travels only implicitly (C fills it in the
        // subscriber's relmap open, not the wire message).
        relkind: 0,
        attkeys,
    })
}

// --- TYPE ---

pub fn logicalrep_write_typ(out: &mut Vec<u8>, xid: TransactionId, typoid: Oid) -> PgResult<()> {
    let basetypoid = lsyscache::getBaseType(typoid)?;

    send_byte(out, LOGICAL_REP_MSG_TYPE);
    if xid != InvalidTransactionId {
        send_int32(out, xid);
    }

    let shape = syscache_seams::pg_type_domain_shape::call(basetypoid)?
        .unwrap_or_else(|| panic!("cache lookup failed for type {basetypoid}"));

    send_int32(out, typoid);
    logicalrep_write_namespace(out, shape.typnamespace)?;
    let typname = String::from_utf8_lossy(shape.typname.name_str()).into_owned();
    send_string(out, &typname);
    Ok(())
}

pub fn logicalrep_read_typ(r: &mut Reader<'_>) -> PgResult<LogicalRepTyp> {
    Ok(LogicalRepTyp {
        remoteid: r.get_int32()?,
        nspname: logicalrep_read_namespace(r)?,
        typname: r.get_string()?,
    })
}

// --- tuple payloads ---

fn varatt_is_external_ondisk(val: Datum) -> bool {
    let p = val.as_usize() as *const u8;
    // SAFETY: a live varlena datum; the first two bytes classify it.
    unsafe { *p == 0x01 && *p.add(1) == varatt::VARTAG_ONDISK }
}

fn oid_output_function_call(mcx: Mcx<'_>, typoutput: Oid, val: Datum) -> PgResult<Vec<u8>> {
    let mut flinfo = fmgr_seams::fmgr_info::call(typoutput)?;
    let d = types_fmgr::function_call1_coll_in(&mut flinfo, InvalidOid, mcx, val)?;
    // SAFETY: output functions return a NUL-terminated cstring datum.
    let bytes =
        unsafe { core::ffi::CStr::from_ptr((d.as_usize() as *const u8).cast()) }.to_bytes();
    Ok(bytes.to_vec())
}

// OidSendFunctionCall: returns the bytea payload (without header).
fn oid_send_function_call(mcx: Mcx<'_>, typsend: Oid, val: Datum) -> PgResult<Vec<u8>> {
    let mut flinfo = fmgr_seams::fmgr_info::call(typsend)?;
    let d = types_fmgr::function_call1_coll_in(&mut flinfo, InvalidOid, mcx, val)?;
    let p = d.as_usize() as *const u8;
    // SAFETY: send functions return an in-line varlena (bytea), never external.
    unsafe {
        debug_assert!(!varatt::varatt_is_1b_e(p));
        let (off, total) = if varatt::varatt_is_1b(p) {
            (varatt::VARHDRSZ_SHORT, varatt::varsize_1b(p))
        } else {
            (varatt::VARHDRSZ, varatt::varsize_4b(p))
        };
        let raw = core::slice::from_raw_parts(p, total);
        Ok(raw[off..].to_vec())
    }
}

// logicalrep_write_tuple (proto.c:766). C converts the change tuple to a
// TupleTableSlot and reads tts_values; here the reorderbuffer's HeapTupleData
// is read directly through heap_getattr (identical attribute values).
fn logicalrep_write_tuple(
    out: &mut Vec<u8>,
    rel: &RelationData<'static>,
    tuple: &HeapTupleData<'_>,
    binary: bool,
    columns: Option<&[i16]>,
    include_gencols_type: u8,
) -> PgResult<()> {
    let desc = &rel.rd_att;

    let mut nliveatts: u16 = 0;
    for i in 0..desc.natts as usize {
        if logicalrep_should_publish_column(desc.attr(i), columns, include_gencols_type) {
            nliveatts += 1;
        }
    }
    send_int16(out, nliveatts);

    // Per-column text/binary conversions palloc; a short-lived arena matches
    // C's use of the caller-reset context.
    let ctx = mcx::MemoryContext::new("logicalrep_write_tuple");
    let mcx = ctx.mcx();

    for i in 0..desc.natts as usize {
        let att = desc.attr(i);
        if !logicalrep_should_publish_column(att, columns, include_gencols_type) {
            continue;
        }

        let mut isnull = false;
        // SAFETY: i+1 <= natts; desc is the tuple's own descriptor.
        let origval = unsafe {
            types_tuple::getattr::heap_getattr(tuple, i as i32 + 1, desc, &mut isnull)
        };

        if isnull {
            send_byte(out, LOGICALREP_COLUMN_NULL);
            continue;
        }

        if att.attlen == -1 && varatt_is_external_ondisk(origval) {
            // Unchanged toasted datum: don't send the value.
            send_byte(out, LOGICALREP_COLUMN_UNCHANGED);
            continue;
        }

        // Detoast in-line/compressed varlenas: C's out/send functions detoast
        // via PG_GETARG; the port's fmgr calls take the datum as-is.
        let val = if att.attlen == -1 {
            let p = origval.as_usize() as *const u8;
            // SAFETY: a live varlena readable through its full VARSIZE_ANY.
            let raw = unsafe { core::slice::from_raw_parts(p, varatt::varsize_any(p)) };
            let detoasted = detoast::detoast_attr(mcx, raw)?;
            let v = Datum::from_usize(detoasted.as_ptr() as usize);
            core::mem::forget(detoasted);
            v
        } else {
            origval
        };

        if binary {
            if let Ok((typsend, _)) = lsyscache::getTypeBinaryOutputInfo(att.atttypid) {
                send_byte(out, LOGICALREP_COLUMN_BINARY);
                let bytes = oid_send_function_call(mcx, typsend, val)?;
                send_int32(out, bytes.len() as u32);
                out.extend_from_slice(&bytes);
                continue;
            }
        }

        let (typoutput, _) = lsyscache::getTypeOutputInfo(att.atttypid)?;
        send_byte(out, LOGICALREP_COLUMN_TEXT);
        let outputstr = oid_output_function_call(mcx, typoutput, val)?;
        send_countedtext(out, &outputstr);
    }
    Ok(())
}

// logicalrep_read_tuple (proto.c:860).
fn logicalrep_read_tuple(r: &mut Reader<'_>) -> PgResult<LogicalRepTupleData> {
    let natts = r.get_int16()? as usize;
    let mut tuple = LogicalRepTupleData {
        colvalues: Vec::with_capacity(natts),
        colstatus: Vec::with_capacity(natts),
        ncols: natts,
    };
    for _ in 0..natts {
        let kind = r.get_byte()?;
        tuple.colstatus.push(kind);
        match kind {
            LOGICALREP_COLUMN_NULL | LOGICALREP_COLUMN_UNCHANGED => {
                tuple.colvalues.push(None);
            }
            LOGICALREP_COLUMN_TEXT | LOGICALREP_COLUMN_BINARY => {
                let len = r.get_int32()? as usize;
                tuple.colvalues.push(Some(r.get_bytes(len)?));
            }
            other => {
                elog(
                    ERROR,
                    format!("unrecognized data representation type '{}'", other as char),
                )?;
                unreachable!();
            }
        }
    }
    Ok(tuple)
}

// logicalrep_write_attrs (proto.c:920).
fn logicalrep_write_attrs(
    out: &mut Vec<u8>,
    rel: &RelationData<'static>,
    columns: Option<&[i16]>,
    include_gencols_type: u8,
) -> PgResult<()> {
    let desc = &rel.rd_att;

    let mut nliveatts: u16 = 0;
    for i in 0..desc.natts as usize {
        if logicalrep_should_publish_column(desc.attr(i), columns, include_gencols_type) {
            nliveatts += 1;
        }
    }
    send_int16(out, nliveatts);

    // Bitmap of REPLICA IDENTITY attributes (RelationGetIdentityKeyBitmap):
    // the relcache's identity-key attnum list serves the same role.
    let replidentfull = rel.rd_rel.relreplident == REPLICA_IDENTITY_FULL;
    let idattrs = if !replidentfull {
        Some(relcache_seams::relation_get_index_attr_bitmap::call(rel.rd_id)?)
    } else {
        None
    };

    for i in 0..desc.natts as usize {
        let att = desc.attr(i);
        if !logicalrep_should_publish_column(att, columns, include_gencols_type) {
            continue;
        }

        let mut flags: u8 = 0;
        // REPLICA IDENTITY FULL means all columns are sent as part of key.
        if replidentfull
            || idattrs
                .as_ref()
                .is_some_and(|bm| bm.identity.contains(&att.attnum))
        {
            flags |= LOGICALREP_IS_REPLICA_IDENTITY;
        }

        send_byte(out, flags);
        let attname = String::from_utf8_lossy(att.attname.name_str()).into_owned();
        send_string(out, &attname);
        send_int32(out, att.atttypid);
        send_int32(out, att.atttypmod as u32);
    }
    Ok(())
}

// logicalrep_read_attrs (proto.c:984).
#[allow(clippy::type_complexity)]
fn logicalrep_read_attrs(
    r: &mut Reader<'_>,
) -> PgResult<(usize, Vec<String>, Vec<Oid>, Vec<bool>)> {
    let natts = r.get_int16()? as usize;
    let mut attnames = Vec::with_capacity(natts);
    let mut atttyps = Vec::with_capacity(natts);
    let mut attkeys = vec![false; natts];

    for key in attkeys.iter_mut() {
        let flags = r.get_byte()?;
        if flags & LOGICALREP_IS_REPLICA_IDENTITY != 0 {
            *key = true;
        }
        attnames.push(r.get_string()?);
        atttyps.push(r.get_int32()?);
        let _attypmod = r.get_int32()?; // ignored, as in C
    }
    Ok((natts, attnames, atttyps, attkeys))
}

// PG_CATALOG_NAMESPACE oid (namespace.h).
const PG_CATALOG_NAMESPACE: Oid = 11;

// logicalrep_write_namespace: empty string stands for pg_catalog.
fn logicalrep_write_namespace(out: &mut Vec<u8>, nspid: Oid) -> PgResult<()> {
    if nspid == PG_CATALOG_NAMESPACE {
        send_byte(out, 0);
    } else {
        let ctx = mcx::MemoryContext::new("logicalrep_write_namespace");
        let nspname = lsyscache::get_namespace_name(ctx.mcx(), nspid)?
            .unwrap_or_else(|| panic!("cache lookup failed for namespace {nspid}"));
        send_string(out, nspname.as_str());
    }
    Ok(())
}

fn logicalrep_read_namespace(r: &mut Reader<'_>) -> PgResult<String> {
    let nspname = r.get_string()?;
    Ok(if nspname.is_empty() {
        "pg_catalog".to_string()
    } else {
        nspname
    })
}

// --- streaming messages (proto.c:1058) ---

// logicalrep_write_stream_start (proto.c:1061).
pub fn logicalrep_write_stream_start(out: &mut Vec<u8>, xid: TransactionId, first_segment: bool) {
    send_byte(out, LOGICAL_REP_MSG_STREAM_START);
    debug_assert!(xid != InvalidTransactionId);
    send_int32(out, xid);
    // 1 if this is the first streaming segment for this xid.
    send_byte(out, if first_segment { 1 } else { 0 });
}

// logicalrep_read_stream_start (proto.c:1079): (xid, first_segment).
pub fn logicalrep_read_stream_start(r: &mut Reader<'_>) -> PgResult<(TransactionId, bool)> {
    let xid = r.get_int32()?;
    let first_segment = r.get_byte()? == 1;
    Ok((xid, first_segment))
}

// logicalrep_write_stream_stop (proto.c:1095).
pub fn logicalrep_write_stream_stop(out: &mut Vec<u8>) {
    send_byte(out, LOGICAL_REP_MSG_STREAM_STOP);
}

// logicalrep_write_stream_commit (proto.c:1104).
pub fn logicalrep_write_stream_commit(
    out: &mut Vec<u8>,
    xid: TransactionId,
    commit_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
    commit_time: TimestampTz,
) {
    send_byte(out, LOGICAL_REP_MSG_STREAM_COMMIT);
    debug_assert!(xid != InvalidTransactionId);
    send_int32(out, xid);
    // flags field (unused for now)
    send_byte(out, 0);
    send_int64(out, commit_lsn);
    send_int64(out, end_lsn);
    send_int64(out, commit_time as u64);
}

// logicalrep_read_stream_commit (proto.c:1129): (xid, commit data).
pub fn logicalrep_read_stream_commit(
    r: &mut Reader<'_>,
) -> PgResult<(TransactionId, LogicalRepCommitData)> {
    let xid = r.get_int32()?;
    let flags = r.get_byte()?;
    if flags != 0 {
        elog(ERROR, format!("unrecognized flags {flags} in commit message"))?;
    }
    Ok((
        xid,
        LogicalRepCommitData {
            commit_lsn: r.get_int64()?,
            end_lsn: r.get_int64()?,
            committime: r.get_int64()? as TimestampTz,
        },
    ))
}

// LogicalRepStreamAbortData (logicalproto.h).
pub struct LogicalRepStreamAbortData {
    pub xid: TransactionId,
    pub subxid: TransactionId,
    pub abort_lsn: XLogRecPtr,
    pub abort_time: TimestampTz,
}

// logicalrep_write_stream_abort (proto.c:1158). xid == subxid for a top-level
// abort; abort_lsn/abort_time ride only when write_abort_info (protocol >= 4
// with parallel streaming).
pub fn logicalrep_write_stream_abort(
    out: &mut Vec<u8>,
    xid: TransactionId,
    subxid: TransactionId,
    abort_lsn: XLogRecPtr,
    abort_time: TimestampTz,
    write_abort_info: bool,
) {
    send_byte(out, LOGICAL_REP_MSG_STREAM_ABORT);
    debug_assert!(xid != InvalidTransactionId && subxid != InvalidTransactionId);
    send_int32(out, xid);
    send_int32(out, subxid);
    if write_abort_info {
        send_int64(out, abort_lsn);
        send_int64(out, abort_time as u64);
    }
}

// logicalrep_read_stream_abort (proto.c:1184).
pub fn logicalrep_read_stream_abort(
    r: &mut Reader<'_>,
    read_abort_info: bool,
) -> PgResult<LogicalRepStreamAbortData> {
    let xid = r.get_int32()?;
    let subxid = r.get_int32()?;
    let (abort_lsn, abort_time) = if read_abort_info {
        (r.get_int64()?, r.get_int64()? as TimestampTz)
    } else {
        (InvalidXLogRecPtr, 0)
    };
    Ok(LogicalRepStreamAbortData { xid, subxid, abort_lsn, abort_time })
}
// --- two-phase (prepare family) messages ---

/// GIDSIZE (xact.h): the on-wire gid is read into a 200-byte buffer in C
/// (strlcpy truncation); readers mirror the cap.
pub const GIDSIZE: usize = 200;

fn gid_truncate(s: String) -> String {
    if s.len() < GIDSIZE {
        return s;
    }
    // strlcpy keeps the first GIDSIZE-1 bytes; stay on a char boundary.
    let mut end = GIDSIZE - 1;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    let mut s = s;
    s.truncate(end);
    s
}

/// LogicalRepPreparedTxnData (logicalproto.h): BEGIN PREPARE / PREPARE.
#[derive(Clone)]
pub struct LogicalRepPreparedTxnData {
    pub prepare_lsn: XLogRecPtr,
    pub end_lsn: XLogRecPtr,
    pub prepare_time: TimestampTz,
    pub xid: TransactionId,
    pub gid: String,
}

/// LogicalRepCommitPreparedTxnData (logicalproto.h).
#[derive(Clone)]
pub struct LogicalRepCommitPreparedTxnData {
    pub commit_lsn: XLogRecPtr,
    pub end_lsn: XLogRecPtr,
    pub commit_time: TimestampTz,
    pub xid: TransactionId,
    pub gid: String,
}

/// LogicalRepRollbackPreparedTxnData (logicalproto.h).
#[derive(Clone)]
pub struct LogicalRepRollbackPreparedTxnData {
    pub prepare_end_lsn: XLogRecPtr,
    pub rollback_end_lsn: XLogRecPtr,
    pub prepare_time: TimestampTz,
    pub rollback_time: TimestampTz,
    pub xid: TransactionId,
    pub gid: String,
}

/// logicalrep_write_begin_prepare (proto.c:116): fields are C's
/// txn->final_lsn, txn->end_lsn, txn->xact_time.prepare_time, txn->xid,
/// txn->gid.
pub fn logicalrep_write_begin_prepare(
    out: &mut Vec<u8>,
    prepare_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
    prepare_time: TimestampTz,
    xid: TransactionId,
    gid: &str,
) {
    send_byte(out, LOGICAL_REP_MSG_BEGIN_PREPARE);
    send_int64(out, prepare_lsn);
    send_int64(out, end_lsn);
    send_int64(out, prepare_time as u64);
    send_int32(out, xid);
    send_string(out, gid);
}

pub fn logicalrep_read_begin_prepare(
    r: &mut Reader<'_>,
) -> PgResult<LogicalRepPreparedTxnData> {
    let prepare_lsn = r.get_int64()?;
    if prepare_lsn == InvalidXLogRecPtr {
        elog(ERROR, "prepare_lsn not set in begin prepare message".to_string())?;
    }
    let end_lsn = r.get_int64()?;
    if end_lsn == InvalidXLogRecPtr {
        elog(ERROR, "end_lsn not set in begin prepare message".to_string())?;
    }
    Ok(LogicalRepPreparedTxnData {
        prepare_lsn,
        end_lsn,
        prepare_time: r.get_int64()? as TimestampTz,
        xid: r.get_int32()?,
        gid: gid_truncate(r.get_string()?),
    })
}

/// logicalrep_write_prepare_common (proto.c:155).
fn logicalrep_write_prepare_common(
    out: &mut Vec<u8>,
    msgtype: u8,
    prepare_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
    prepare_time: TimestampTz,
    xid: TransactionId,
    gid: &str,
) {
    send_byte(out, msgtype);
    debug_assert!(xid != InvalidTransactionId);
    send_byte(out, 0); // flags
    send_int64(out, prepare_lsn);
    send_int64(out, end_lsn);
    send_int64(out, prepare_time as u64);
    send_int32(out, xid);
    send_string(out, gid);
}

/// logicalrep_write_prepare (proto.c:187): prepare_lsn is the caller's,
/// end_lsn/prepare_time/xid/gid are C's txn fields.
pub fn logicalrep_write_prepare(
    out: &mut Vec<u8>,
    prepare_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
    prepare_time: TimestampTz,
    xid: TransactionId,
    gid: &str,
) {
    logicalrep_write_prepare_common(
        out,
        LOGICAL_REP_MSG_PREPARE,
        prepare_lsn,
        end_lsn,
        prepare_time,
        xid,
        gid,
    );
}

fn logicalrep_read_prepare_common(
    r: &mut Reader<'_>,
    msgtype: &str,
) -> PgResult<LogicalRepPreparedTxnData> {
    let flags = r.get_byte()?;
    if flags != 0 {
        elog(ERROR, format!("unrecognized flags {flags} in {msgtype} message"))?;
    }
    let prepare_lsn = r.get_int64()?;
    if prepare_lsn == InvalidXLogRecPtr {
        elog(ERROR, format!("prepare_lsn is not set in {msgtype} message"))?;
    }
    let end_lsn = r.get_int64()?;
    if end_lsn == InvalidXLogRecPtr {
        elog(ERROR, format!("end_lsn is not set in {msgtype} message"))?;
    }
    let prepare_time = r.get_int64()? as TimestampTz;
    let xid = r.get_int32()?;
    if xid == InvalidTransactionId {
        elog(
            ERROR,
            format!("invalid two-phase transaction ID in {msgtype} message"),
        )?;
    }
    Ok(LogicalRepPreparedTxnData {
        prepare_lsn,
        end_lsn,
        prepare_time,
        xid,
        gid: gid_truncate(r.get_string()?),
    })
}

pub fn logicalrep_read_prepare(r: &mut Reader<'_>) -> PgResult<LogicalRepPreparedTxnData> {
    logicalrep_read_prepare_common(r, "prepare")
}

/// logicalrep_write_commit_prepared (proto.c:237): end_lsn/commit_time/xid/gid
/// are C's txn fields.
pub fn logicalrep_write_commit_prepared(
    out: &mut Vec<u8>,
    commit_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
    commit_time: TimestampTz,
    xid: TransactionId,
    gid: &str,
) {
    send_byte(out, LOGICAL_REP_MSG_COMMIT_PREPARED);
    send_byte(out, 0); // flags
    send_int64(out, commit_lsn);
    send_int64(out, end_lsn);
    send_int64(out, commit_time as u64);
    send_int32(out, xid);
    send_string(out, gid);
}

pub fn logicalrep_read_commit_prepared(
    r: &mut Reader<'_>,
) -> PgResult<LogicalRepCommitPreparedTxnData> {
    let flags = r.get_byte()?;
    if flags != 0 {
        elog(
            ERROR,
            format!("unrecognized flags {flags} in commit prepared message"),
        )?;
    }
    let commit_lsn = r.get_int64()?;
    if commit_lsn == InvalidXLogRecPtr {
        elog(ERROR, "commit_lsn is not set in commit prepared message".to_string())?;
    }
    let end_lsn = r.get_int64()?;
    if end_lsn == InvalidXLogRecPtr {
        elog(ERROR, "end_lsn is not set in commit prepared message".to_string())?;
    }
    Ok(LogicalRepCommitPreparedTxnData {
        commit_lsn,
        end_lsn,
        commit_time: r.get_int64()? as TimestampTz,
        xid: r.get_int32()?,
        gid: gid_truncate(r.get_string()?),
    })
}

/// logicalrep_write_rollback_prepared (proto.c:293): rollback_end_lsn and
/// rollback_time are C's txn->end_lsn / txn->xact_time.commit_time.
#[allow(clippy::too_many_arguments)]
pub fn logicalrep_write_rollback_prepared(
    out: &mut Vec<u8>,
    prepare_end_lsn: XLogRecPtr,
    rollback_end_lsn: XLogRecPtr,
    prepare_time: TimestampTz,
    rollback_time: TimestampTz,
    xid: TransactionId,
    gid: &str,
) {
    send_byte(out, LOGICAL_REP_MSG_ROLLBACK_PREPARED);
    send_byte(out, 0); // flags
    send_int64(out, prepare_end_lsn);
    send_int64(out, rollback_end_lsn);
    send_int64(out, prepare_time as u64);
    send_int64(out, rollback_time as u64);
    send_int32(out, xid);
    send_string(out, gid);
}

pub fn logicalrep_read_rollback_prepared(
    r: &mut Reader<'_>,
) -> PgResult<LogicalRepRollbackPreparedTxnData> {
    let flags = r.get_byte()?;
    if flags != 0 {
        elog(
            ERROR,
            format!("unrecognized flags {flags} in rollback prepared message"),
        )?;
    }
    let prepare_end_lsn = r.get_int64()?;
    if prepare_end_lsn == InvalidXLogRecPtr {
        elog(
            ERROR,
            "prepare_end_lsn is not set in rollback prepared message".to_string(),
        )?;
    }
    let rollback_end_lsn = r.get_int64()?;
    if rollback_end_lsn == InvalidXLogRecPtr {
        elog(
            ERROR,
            "rollback_end_lsn is not set in rollback prepared message".to_string(),
        )?;
    }
    Ok(LogicalRepRollbackPreparedTxnData {
        prepare_end_lsn,
        rollback_end_lsn,
        prepare_time: r.get_int64()? as TimestampTz,
        rollback_time: r.get_int64()? as TimestampTz,
        xid: r.get_int32()?,
        gid: gid_truncate(r.get_string()?),
    })
}
pub fn logicalrep_write_stream_prepare(_out: &mut Vec<u8>) -> ! {
    unported("logicalrep_write_stream_prepare (streaming two-phase)")
}
pub fn logicalrep_read_stream_prepare(_r: &mut Reader<'_>) -> ! {
    unported("logicalrep_read_stream_prepare (streaming two-phase)")
}

/// logicalrep_message_type (proto.c:1209).
pub fn logicalrep_message_type(action: u8) -> String {
    match action {
        LOGICAL_REP_MSG_BEGIN => "BEGIN".to_string(),
        LOGICAL_REP_MSG_COMMIT => "COMMIT".to_string(),
        LOGICAL_REP_MSG_ORIGIN => "ORIGIN".to_string(),
        LOGICAL_REP_MSG_INSERT => "INSERT".to_string(),
        LOGICAL_REP_MSG_UPDATE => "UPDATE".to_string(),
        LOGICAL_REP_MSG_DELETE => "DELETE".to_string(),
        LOGICAL_REP_MSG_TRUNCATE => "TRUNCATE".to_string(),
        LOGICAL_REP_MSG_RELATION => "RELATION".to_string(),
        LOGICAL_REP_MSG_TYPE => "TYPE".to_string(),
        LOGICAL_REP_MSG_MESSAGE => "MESSAGE".to_string(),
        LOGICAL_REP_MSG_BEGIN_PREPARE => "BEGIN PREPARE".to_string(),
        LOGICAL_REP_MSG_PREPARE => "PREPARE".to_string(),
        LOGICAL_REP_MSG_COMMIT_PREPARED => "COMMIT PREPARED".to_string(),
        LOGICAL_REP_MSG_ROLLBACK_PREPARED => "ROLLBACK PREPARED".to_string(),
        LOGICAL_REP_MSG_STREAM_START => "STREAM START".to_string(),
        LOGICAL_REP_MSG_STREAM_STOP => "STREAM STOP".to_string(),
        LOGICAL_REP_MSG_STREAM_COMMIT => "STREAM COMMIT".to_string(),
        LOGICAL_REP_MSG_STREAM_ABORT => "STREAM ABORT".to_string(),
        LOGICAL_REP_MSG_STREAM_PREPARE => "STREAM PREPARE".to_string(),
        other => format!("??? ({other})"),
    }
}

/// logicalrep_should_publish_column (proto.c:1279). `columns` is the
/// publication column list as raw attnums (C's Bitmapset, unshifted).
pub fn logicalrep_should_publish_column(
    att: &FormData_pg_attribute,
    columns: Option<&[i16]>,
    include_gencols_type: u8,
) -> bool {
    if att.attisdropped {
        return false;
    }

    // If a column list is provided, publish only the cols in that list.
    if let Some(cols) = columns {
        return cols.contains(&att.attnum);
    }

    // All non-generated columns are always published.
    if att.attgenerated == 0 {
        return true;
    }

    // Stored generated columns are only published when the user sets
    // publish_generated_columns as stored.
    if att.attgenerated == ATTRIBUTE_GENERATED_STORED {
        return include_gencols_type == PUBLISH_GENCOLS_STORED;
    }

    false
}

#[cfg(test)]
mod tests;
